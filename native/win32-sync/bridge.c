#undef _WIN32_WINNT
#define _WIN32_WINNT 0x0601
#define WINVER 0x0601
#include <windows.h>
#include "runtime.h"

#define BUILD "1.0.44-sync3"
#define LINE_MAXIMUM (2*1024*1024)
#define QUEUE_MAXIMUM 256
#define BYTE_MAXIMUM (6*1024*1024)
#define DATA_MAXIMUM (1024*1024)
static HANDLE serial = INVALID_HANDLE_VALUE, parent;
static CRITICAL_SECTION queueLock;
static char *queue[QUEUE_MAXIMUM];
static unsigned head, count, queuedBytes;
static HANDLE ioThread;
static volatile LONG closingRequested, reading;
static volatile LONG alive = 1, pulse;
static const char b64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* Protocol input is deliberately flat JSON with ASCII keys/strings, numbers and booleans.
   The adapter validates COM names; binary payloads are Base64, so no JSON escaping is needed. */
typedef struct { char key[32]; char *value; int quoted; } Field;
typedef struct { Field f[16]; int n; } Message;
static void skip(char **p) { while (**p && isspace((unsigned char)**p)) ++*p; }
static int parse(char *s, Message *m) {
    char *p=s; m->n=0; skip(&p); if (*p++!='{') return 0;
    for (;;) {
        skip(&p); if (*p=='}') { ++p; skip(&p); return !*p; }
        if (m->n==16 || *p++!='"') return 0;
        char *k=p; while (*p && *p!='"') { if (*p=='\\') return 0; p++; }
        if (!*p || p-k>31) return 0;
        Field *f=&m->f[m->n++]; memcpy(f->key,k,(size_t)(p-k)); f->key[p-k]=0; p++;
        for(int j=0;j<m->n-1;j++) if(!strcmp(m->f[j].key,f->key)) return 0;
        skip(&p); if (*p++!=':') return 0; skip(&p);
        f->quoted=(*p=='"'); if(f->quoted)p++; f->value=p;
        if(f->quoted) {
            while(*p && *p!='"') { if(*p=='\\' || (unsigned char)*p<32)return 0; p++; }
            if(!*p)return 0; *p++=0; skip(&p);
        } else {
            while(*p && *p!=',' && *p!='}' && !isspace((unsigned char)*p))p++;
            if(p==f->value)return 0;
            char delim=*p; if(!delim)return 0; *p++=0;
            if(delim=='}'){skip(&p);return !*p;}
            if(delim==',')continue;
            skip(&p);
        }
        if(*p==','){p++;continue;}
        if(*p=='}'){p++;skip(&p);return !*p;}
        return 0;
    }
}
static Field *field(Message *m,const char *key){for(int i=0;i<m->n;i++)if(!strcmp(m->f[i].key,key))return &m->f[i];return NULL;}
static const char *str(Message *m,const char *key){Field *f=field(m,key);return f&&f->quoted?f->value:NULL;}
static double number(Message *m,const char *key){Field *f=field(m,key);char *end; if(!f||f->quoted)return -1;double n=strtod(f->value,&end);return *end?-1:n;}
static int boolValue(Message *m,const char *key){Field *f=field(m,key);return f&&!f->quoted&&!strcmp(f->value,"true");}
static void output(const char *s){ if(fputs(s,stdout)<0 || fflush(stdout))ExitProcess(3); }
static void reply(unsigned id,int ok,const char *phase,DWORD code,unsigned written){
    char line[400];
    snprintf(line,sizeof(line),"{\"v\":1,\"type\":\"reply\",\"id\":%u,\"ok\":%s,\"phase\":\"%s\",\"win32Code\":%lu,\"written\":%u,\"message\":\"%s Win32=%lu\"}\n",id,ok?"true":"false",phase,(unsigned long)code,written,phase,(unsigned long)code);
    output(line);
}
static void fatal(const char *phase,DWORD code){char line[300];snprintf(line,sizeof(line),"{\"v\":1,\"type\":\"fatal\",\"phase\":\"%s\",\"win32Code\":%lu,\"message\":\"%s Win32=%lu\"}\n",phase,(unsigned long)code,phase,(unsigned long)code);output(line);ExitProcess(1);}
static void rx(const unsigned char *data,DWORD n){
    char out[6000];size_t j=0; j+=(size_t)sprintf(out,"{\"v\":1,\"type\":\"data\",\"data\":\"");
    for(DWORD i=0;i<n;i+=3){unsigned v=(unsigned)data[i]<<16;if(i+1<n)v|=(unsigned)data[i+1]<<8;if(i+2<n)v|=data[i+2];out[j++]=b64[v>>18];out[j++]=b64[(v>>12)&63];out[j++]=i+1<n?b64[(v>>6)&63]:'=';out[j++]=i+2<n?b64[v&63]:'=';}
    strcpy(out+j,"\"}\n");output(out);
}
static int sextet(char c){const char *p=c?strchr(b64,c):NULL;return p?(int)(p-b64):-1;}
static unsigned char *decode(const char *s,DWORD *length){
    if(!s)return NULL;size_t len=strlen(s);if(len%4 || len>(DATA_MAXIMUM+2)/3*4)return NULL;
    unsigned char *out=malloc(len/4*3+1);if(!out)return NULL;DWORD n=0;
    for(size_t i=0;i<len;i+=4){int a=sextet(s[i]),b=sextet(s[i+1]),c=sextet(s[i+2]),d=sextet(s[i+3]);
        int pad2=s[i+2]=='=',pad1=s[i+3]=='=';
        if(a<0||b<0||(!pad2&&c<0)||(!pad1&&d<0)||(pad2&&!pad1)||((pad1||pad2)&&i+4!=len)||(pad2&&(b&15))||(!pad2&&pad1&&(c&3))){free(out);return NULL;}
        out[n++]=(unsigned char)((a<<2)|(b>>4));if(!pad2)out[n++]=(unsigned char)((b<<4)|(c>>2));if(!pad1)out[n++]=(unsigned char)((c<<6)|d);
    }
    if(n>DATA_MAXIMUM){free(out);return NULL;}*length=n;return out;
}
static DWORD WINAPI reader(LPVOID ignored){
    (void)ignored;char *line=malloc(LINE_MAXIMUM+1);if(!line)ExitProcess(4);size_t n=0;
    int c;while((c=getchar())!=EOF){if(c=='\n'){
        line[n]=0;
        /* Inspect the small close command without modifying the queued JSON. */
        if(n<128){
            char copy[128];Message m;memcpy(copy,line,n+1);
            if(parse(copy,&m) && str(&m,"op") && !strcmp(str(&m,"op"),"close"))
                InterlockedExchange(&closingRequested,1);
        }
        EnterCriticalSection(&queueLock);
        if(count>=QUEUE_MAXIMUM || queuedBytes+n>BYTE_MAXIMUM){LeaveCriticalSection(&queueLock);ExitProcess(5);}
        char *copy=malloc(n+1);if(!copy){LeaveCriticalSection(&queueLock);ExitProcess(4);}memcpy(copy,line,n+1);
        queue[(head+count)%QUEUE_MAXIMUM]=copy;count++;queuedBytes+=(unsigned)n;LeaveCriticalSection(&queueLock);n=0;
    }else{if(n>=LINE_MAXIMUM)ExitProcess(5);line[n++]=(char)c;}}
    free(line);InterlockedExchange(&alive,0);return 0;
}
static DWORD WINAPI watchdog(LPVOID ignored){
    (void)ignored;for(;;){Sleep(500);
        if(WaitForSingleObject(parent,0)==WAIT_OBJECT_0)InterlockedExchange(&alive,0);
        if(!InterlockedCompareExchange(&alive,1,1) ||
           (InterlockedCompareExchange(&closingRequested,0,0) && InterlockedCompareExchange(&reading,0,0)))
            CancelSynchronousIo(ioThread);
        if((DWORD)(GetTickCount()-(DWORD)InterlockedCompareExchange(&pulse,0,0))>10000)ExitProcess(124);
    }return 0;
}
static DWORD configure(Message *m,const char **phase){
    double baud=number(m,"baud"),bits=number(m,"bits"),stops=number(m,"stops");const char *parity=str(m,"parity"),*port=str(m,"port");
    if(!port||strlen(port)>24||strncmp(port,"COM",3)||!port[3]||baud<1||baud>4294967295.0||baud!=(DWORD)baud||bits<5||bits>8||bits!=(BYTE)bits||!(stops==1||stops==1.5||stops==2)||!parity)return ERROR_INVALID_PARAMETER;
    for(const char *p=port+3;*p;p++)if(!isdigit((unsigned char)*p))return ERROR_INVALID_PARAMETER;
    BYTE par;if(!strcmp(parity,"none"))par=NOPARITY;else if(!strcmp(parity,"even"))par=EVENPARITY;else if(!strcmp(parity,"odd"))par=ODDPARITY;else if(!strcmp(parity,"mark"))par=MARKPARITY;else if(!strcmp(parity,"space"))par=SPACEPARITY;else return ERROR_INVALID_PARAMETER;
    char name[32];snprintf(name,sizeof(name),"\\\\.\\%s",port);
    *phase="CreateFileA";
    serial=CreateFileA(name,GENERIC_READ|GENERIC_WRITE,0,NULL,OPEN_EXISTING,0,NULL);
    if(serial==INVALID_HANDLE_VALUE)return GetLastError();
    DCB d;memset(&d,0,sizeof(d));d.DCBlength=sizeof(d);DWORD code=0;
    *phase="GetCommState";
    if(!GetCommState(serial,&d)){code=GetLastError();goto bad;}
    /* Exactly the successful diagnostic's control flags, retaining character fields. */
    d.BaudRate=(DWORD)baud;d.ByteSize=(BYTE)bits;d.Parity=par;d.StopBits=stops==1?ONESTOPBIT:stops==1.5?ONE5STOPBITS:TWOSTOPBITS;
    d.fBinary=1;d.fParity=par!=NOPARITY;d.fOutxCtsFlow=0;d.fOutxDsrFlow=0;d.fDtrControl=DTR_CONTROL_ENABLE;
    d.fDsrSensitivity=0;d.fTXContinueOnXoff=0;d.fOutX=0;d.fInX=0;d.fErrorChar=0;d.fNull=0;d.fRtsControl=RTS_CONTROL_DISABLE;d.fAbortOnError=0;d.fDummy2=0;
    *phase="SetCommState";
    if(!SetCommState(serial,&d)){code=GetLastError();goto bad;}
    if(GetCommState(serial,&d))fprintf(stderr,"DCB readback baud=%lu parity=%u DTR=%u RTS=%u\n",(unsigned long)d.BaudRate,d.Parity,d.fDtrControl,d.fRtsControl);
    COMMTIMEOUTS t;memset(&t,0,sizeof(t));t.ReadIntervalTimeout=20;t.ReadTotalTimeoutConstant=200;t.WriteTotalTimeoutConstant=3000;
    *phase="SetCommTimeouts";
    if(!SetCommTimeouts(serial,&t)){code=GetLastError();goto bad;}
    if(GetCommTimeouts(serial,&t))fprintf(stderr,"Timeout readback interval=%lu read=%lu write=%lu\n",(unsigned long)t.ReadIntervalTimeout,(unsigned long)t.ReadTotalTimeoutConstant,(unsigned long)t.WriteTotalTimeoutConstant);
    return 0;
bad: CloseHandle(serial);serial=INVALID_HANDLE_VALUE;return code;
}
static void command(char *line){
    Message m;if(!parse(line,&m))fatal("protocol",ERROR_INVALID_DATA);
    double idValue=number(&m,"id");if(number(&m,"v")!=1||idValue<1||idValue>4294967295.0||idValue!=(unsigned)idValue)fatal("protocol",ERROR_INVALID_DATA);
    unsigned id=(unsigned)idValue;const char *op=str(&m,"op");if(!op)fatal("protocol",ERROR_INVALID_DATA);
    const char *phase=op;
    DWORD code=0,written=0;
    if(!strcmp(op,"close")){
        if(serial!=INVALID_HANDLE_VALUE){if(!CloseHandle(serial))code=GetLastError();serial=INVALID_HANDLE_VALUE;}
        reply(id,!code,"close",code,0);ExitProcess(code?1:0);
    } else if(!strcmp(op,"open")) {
        code=serial==INVALID_HANDLE_VALUE?configure(&m,&phase):ERROR_ALREADY_EXISTS;
    } else if(serial==INVALID_HANDLE_VALUE)code=ERROR_INVALID_HANDLE;
    else if(!strcmp(op,"write")){
        DWORD length;unsigned char *data=decode(str(&m,"data"),&length);if(!data)code=ERROR_INVALID_DATA;
        else {DWORD start=GetTickCount();while(written<length){DWORD n=0;
            if(!WriteFile(serial,data+written,length-written,&n,NULL)){code=GetLastError();break;}
            written+=n;if(!n){code=ERROR_WRITE_FAULT;break;}
            if(GetTickCount()-start>3000 && written<length){code=ERROR_TIMEOUT;break;}
        }free(data);}
    } else if(!strcmp(op,"update")){
        double baud=number(&m,"baud");DCB d;memset(&d,0,sizeof(d));d.DCBlength=sizeof(d);
        if(baud<1||baud>4294967295.0||baud!=(DWORD)baud)code=ERROR_INVALID_PARAMETER;
        else if(!GetCommState(serial,&d))code=GetLastError();else{d.BaudRate=(DWORD)baud;if(!SetCommState(serial,&d))code=GetLastError();}
    } else if(!strcmp(op,"set")){
        if(!EscapeCommFunction(serial,boolValue(&m,"rts")?SETRTS:CLRRTS))code=GetLastError();
        if(!code&&!EscapeCommFunction(serial,boolValue(&m,"dtr")?SETDTR:CLRDTR))code=GetLastError();
        if(!code&&!EscapeCommFunction(serial,boolValue(&m,"brk")?SETBREAK:CLRBREAK))code=GetLastError();
        DWORD mask=0;if(!code&&!GetCommMask(serial,&mask))code=GetLastError();
        mask&=~(EV_CTS|EV_DSR);if(boolValue(&m,"cts"))mask|=EV_CTS;if(boolValue(&m,"dsr"))mask|=EV_DSR;
        if(!code&&!SetCommMask(serial,mask))code=GetLastError();
    } else if(!strcmp(op,"flush")){
        if(!PurgeComm(serial,PURGE_RXCLEAR|PURGE_TXCLEAR|PURGE_RXABORT|PURGE_TXABORT))code=GetLastError();
    } else code=ERROR_INVALID_FUNCTION;
    reply(id,!code,phase,code,written);
    if(code && !strcmp(op,"open"))ExitProcess(1);
}
int main(int argc,char **argv){
    if(argc!=2)return 2;char *end;unsigned long pid=strtoul(argv[1],&end,10);if(*end||!pid)return 2;
    parent=OpenProcess(SYNCHRONIZE,FALSE,(DWORD)pid);if(!parent)return 2;
    _setmode(_fileno(stdin),_O_BINARY);_setmode(_fileno(stdout),_O_BINARY);setvbuf(stderr,NULL,_IONBF,0);
    if(!DuplicateHandle(GetCurrentProcess(),GetCurrentThread(),GetCurrentProcess(),&ioThread,0,FALSE,DUPLICATE_SAME_ACCESS))return 2;
    InitializeCriticalSection(&queueLock);InterlockedExchange(&pulse,(LONG)GetTickCount());
    HANDLE r=CreateThread(NULL,0,reader,NULL,0,NULL),w=CreateThread(NULL,0,watchdog,NULL,0,NULL);if(!r||!w)return 2;CloseHandle(r);CloseHandle(w);
    char hello[160];snprintf(hello,sizeof(hello),"{\"v\":1,\"type\":\"hello\",\"build\":\"%s\",\"bits\":%u}\n",BUILD,(unsigned)(sizeof(void*)*8));output(hello);
    while(InterlockedCompareExchange(&alive,1,1)){
        InterlockedExchange(&pulse,(LONG)GetTickCount());
        for(int i=0;i<16;i++){
            char *line=NULL;EnterCriticalSection(&queueLock);if(count){line=queue[head];head=(head+1)%QUEUE_MAXIMUM;count--;queuedBytes-=(unsigned)strlen(line);}LeaveCriticalSection(&queueLock);
            if(!line)break;command(line);free(line);InterlockedExchange(&pulse,(LONG)GetTickCount());
        }
        if(serial!=INVALID_HANDLE_VALUE){unsigned char data[4096];DWORD n=0;
            InterlockedExchange(&reading,1);
            if(InterlockedCompareExchange(&closingRequested,0,0)){InterlockedExchange(&reading,0);continue;}
            BOOL ok=ReadFile(serial,data,sizeof(data),&n,NULL);
            DWORD readError=ok?0:GetLastError();
            InterlockedExchange(&reading,0);
            if(!ok){
                if(readError==ERROR_OPERATION_ABORTED && (closingRequested || !alive))continue;
                fatal("ReadFile",readError);
            }
            if(n)rx(data,n);else Sleep(5);
        }else Sleep(10);
    }
    if(serial!=INVALID_HANDLE_VALUE)CloseHandle(serial);CloseHandle(parent);return 0;
}

/* The helper receives only the decimal parent PID; no CRT argv parser is needed. */
void mainCRTStartup(void) {
    char *cmd=GetCommandLineA(), *pid=cmd;
    for(char *p=cmd;*p;p++)if(*p==' ')pid=p+1;
    char *args[2]={cmd,pid};
    ExitProcess((UINT)main(2,args));
}
