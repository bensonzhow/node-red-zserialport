/* Minimal Win32-only runtime: no UCRT, VC redistributable or CLR dependency.
   Only the functions required by bridge.c are implemented here. */
#include <stddef.h>
#include <stdint.h>
/* No TLS variables are used; Zig compiler-rt retains this linker reference. */
DWORD _tls_index = 0;
#define EOF (-1)
#define stdin 0
#define stdout 1
#define stderr 2
#define _O_BINARY 0
#define _IONBF 0
#define _fileno(x) (x)
#define _setmode(x,y) ((void)0)
#define setvbuf(a,b,c,d) ((void)0)
#define strlen bridge_strlen
#define strcmp bridge_strcmp
#define strncmp bridge_strncmp
#define strchr bridge_strchr
#define strcpy bridge_strcpy
#define isspace bridge_isspace
#define isdigit bridge_isdigit
#define malloc bridge_malloc
#define free bridge_free
#define strtoul bridge_strtoul
#define strtod bridge_strtod
static HANDLE streamHandle(int fd) { return GetStdHandle(fd==0?STD_INPUT_HANDLE:fd==1?STD_OUTPUT_HANDLE:STD_ERROR_HANDLE); }
void *memcpy(void *dst,const void *src,size_t n){unsigned char *d=dst;const unsigned char *s=src;while(n--)*d++=*s++;return dst;}
void *memset(void *dst,int v,size_t n){unsigned char *d=dst;while(n--)*d++=(unsigned char)v;return dst;}
static size_t strlen(const char *s){const char *p=s;while(*p)p++;return (size_t)(p-s);}
static int strcmp(const char *a,const char *b){while(*a&&*a==*b){a++;b++;}return (unsigned char)*a-(unsigned char)*b;}
static int strncmp(const char *a,const char *b,size_t n){while(n&&*a&&*a==*b){a++;b++;n--;}return n?(unsigned char)*a-(unsigned char)*b:0;}
static char *strchr(const char *s,int c){do{if(*s==c)return (char*)s;}while(*s++);return NULL;}
static char *strcpy(char *d,const char *s){char *p=d;while((*p++=*s++)){}return d;}
static int isspace(int c){return c==' '||c=='\t'||c=='\r'||c=='\n'||c=='\v'||c=='\f';}
static int isdigit(int c){return c>='0'&&c<='9';}
static void *malloc(size_t n){return HeapAlloc(GetProcessHeap(),0,n);}
static void free(void *p){if(p)HeapFree(GetProcessHeap(),0,p);}
static int fputs(const char *s,int fd){DWORD n;size_t len=strlen(s);while(len){if(!WriteFile(streamHandle(fd),s,(DWORD)len,&n,NULL)||!n)return EOF;s+=n;len-=n;}return 0;}
static int fflush(int fd){(void)fd;return 0;}
static int getchar(void){static unsigned char buf[4096];static DWORD pos,n;if(pos==n){pos=0;if(!ReadFile(streamHandle(0),buf,sizeof(buf),&n,NULL)||!n)return EOF;}return buf[pos++];}
/* All format strings are fixed in bridge.c; %s arguments are validated/bounded.
   wsprintfA handles the small integer-only messages without a C runtime. */
#define snprintf(dst,size,...) wsprintfA(dst,__VA_ARGS__)
#define sprintf wsprintfA
#define fprintf(fd,...) do { char text[512]; wsprintfA(text,__VA_ARGS__); fputs(text,fd); } while(0)
static unsigned long strtoul(const char *s,char **end,int base){unsigned long n=0;(void)base;while(isdigit(*s)){unsigned digit=(unsigned)(*s-'0');if(n>(0xffffffffUL-digit)/10){*end=(char*)s;return 0;}n=n*10+digit;s++;}*end=(char*)s;return n;}
/* Protocol numbers are nonnegative integers, except stopBits=1.5. */
static double strtod(const char *s,char **end){const char *start=s;unsigned long n=strtoul(s,end,10);if(*end==start)return -1;double value=(double)n;if((*end)[0]=='.'&&(*end)[1]=='5'){value+=0.5;*end+=2;}return value;}
