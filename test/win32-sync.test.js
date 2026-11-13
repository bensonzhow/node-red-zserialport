'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('vm');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
const { PassThrough, Writable } = require('stream');
const source = fs.readFileSync(path.join(__dirname, '../lib/win32-sync-port.js'), 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
function fixture({platform = 'win32', arch = 'x64', onCommand, spawnError, accessError, handshake = true, stuck = false} = {}) {
    const child = new EventEmitter(), commands = [], timers = new Set();
    child.stdout = new PassThrough(); child.stderr = new PassThrough();
    child.send = message => child.stdout.write(JSON.stringify({v: 1, ...message}) + '\n');
    child.stdin = new Writable({write(data, enc, done) {
        const cmd = JSON.parse(data.toString()); commands.push(cmd); done();
        process.nextTick(() => {
            if (onCommand && onCommand(cmd, child) === false) return;
            child.send({type: 'reply', id: cmd.id, ok: true, written: cmd.data ? Buffer.from(cmd.data, 'base64').length : 0});
            if (cmd.op === 'close') process.nextTick(() => child.emit('close', 0, null));
        });
    }});
    child.stdin.on('finish', () => { if (!stuck) process.nextTick(() => child.emit('close', 0, null)); });
    child.kill = () => { child.killed = true; process.nextTick(() => child.emit('close', 1, null)); };
    let spawnOptions, exe;
    const exports = {exports: {}};
    const realRequire = require;
    const sandbox = {
        module: exports, Buffer, __dirname: path.join(__dirname, '../lib'),
        process: {platform, arch, pid: 123, nextTick: process.nextTick},
        setTimeout: (fn, ms) => { const t = setTimeout(() => {timers.delete(t); fn();}, ms); timers.add(t); return t; },
        clearTimeout: t => {timers.delete(t); clearTimeout(t);},
        require: name => {
            if(name === 'fs') return {constants: fs.constants, accessSync: () => {if(accessError)throw accessError;}};
            if(name === 'child_process') return {spawn: (file, args, opts) => {
                exe = file; spawnOptions = opts;
                process.nextTick(() => {
                    if(spawnError)child.emit('error', spawnError);
                    else if(handshake)child.send({type: 'hello', build: 'test', bits: arch === 'ia32' ? 32 : 64});
                }); return child;
            }};
            return realRequire(name);
        }
    };
    vm.runInNewContext(source, sandbox);
    const Port = exports.exports;
    return {Port, child, commands, timers, get exe(){return exe;}, get spawnOptions(){return spawnOptions;}};
}
async function opened(f) {
    let port;
    await new Promise((resolve, reject) => {port = new f.Port({path:'COM11',baudRate:9600,parity:'EVEN'}, e => e ? reject(e) : resolve());});
    return port;
}
async function closed(port) {await new Promise((resolve,reject)=>port.close(e=>e?reject(e):resolve()));}

test('open config, safe spawn, pre-open write and arbitrary binary RX', async () => {
    const f=fixture();let rx;let calls=0;
    const p=new f.Port({path:'COM11',baudRate:'9600',parity:'EVEN'},e=>assert.ifError(e));
    p.on('data', b=>rx=b);
    const payload=Buffer.from([0,254,255,10,13]);
    await new Promise((resolve,reject)=>p.write(payload,e=>{calls++;e?reject(e):resolve();}));
    assert.equal(calls,1);assert.equal(f.commands[0].parity,'even');assert.equal(f.commands[0].baud,9600);
    assert.equal(f.spawnOptions.shell,false);assert.match(f.exe,/windows[\\/]x64/);
    assert.deepEqual(Buffer.from(f.commands[1].data,'base64'),payload);
    const line=JSON.stringify({v:1,type:'data',data:payload.toString('base64')})+'\n';
    f.child.stdout.write(line.slice(0,8));f.child.stdout.write(line.slice(8));assert.deepEqual(rx,payload);
    await closed(p);assert.equal(f.timers.size,0);
});
test('32-bit Node chooses x86', async()=>{const f=fixture({arch:'ia32'});const p=await opened(f);assert.match(f.exe,/windows[\\/]x86/);await closed(p);});
test('non-Windows and missing binary fail through open callback only', async()=>{
    for(const opts of [{platform:'darwin'},{accessError:new Error('ENOENT')}]){
        const f=fixture(opts);let count=0;const p=new f.Port({path:'COM11',baudRate:9600},e=>{assert(e);count++;});
        p.on('error',()=>assert.fail('unexpected error event'));await tick();assert.equal(count,1);assert.equal(p.isOpen,false);assert.equal(f.timers.size,0);
    }
});
test('native open failure is not duplicated as close/error',async()=>{
    const f=fixture({onCommand:(cmd,c)=>{if(cmd.op==='open'){c.send({type:'reply',id:cmd.id,ok:false,phase:'open',win32Code:5,message:'access denied'});return false;}}});
    let calls=0;const p=new f.Port({path:'COM11',baudRate:9600},e=>{assert.equal(e.win32Code,5);calls++;});
    p.on('error',()=>assert.fail('duplicate'));p.on('close',()=>assert.fail('initial failure close'));
    await tick();await tick();assert.equal(calls,1);assert.equal(f.timers.size,0);
});
test('early RX, partial write, update, set and flush callbacks',async()=>{
    const f=fixture({onCommand:(cmd,c)=>{if(cmd.op==='write'){c.send({type:'data',data:'/wA='});c.send({type:'reply',id:cmd.id,ok:true,written:1});return false;}}});
    const p=await opened(f);let rx;p.on('data',b=>rx=b);
    await new Promise(resolve=>p.write(Buffer.from([1,2]),e=>{assert.equal(e.phase,'write');assert.equal(e.writtenBytes,1);resolve();}));
    assert.deepEqual(rx,Buffer.from([255,0]));
    for(const [op,arg] of [['update',{baudRate:1200}],['set',{rts:false,dtr:true}],['flush',null]])await new Promise((resolve,reject)=>{const cb=e=>e?reject(e):resolve();arg?p[op](arg,cb):p[op](cb);});
    assert.equal(f.commands.find(c=>c.op==='update').baud,1200);
    assert.equal(f.commands.find(c=>c.op==='set').rts,false);
    await closed(p);
});
test('unexpected exit fails pending write once and emits one close',async()=>{
    const f=fixture({onCommand:cmd=>cmd.op==='write'?false:undefined});const p=await opened(f);let errors=0,closes=0,calls=0;
    p.on('error',()=>errors++);p.on('close',()=>closes++);
    p.write(Buffer.from([1]),e=>{assert(e);calls++;});await tick();f.child.emit('close',9,null);await tick();
    assert.equal(errors,1);assert.equal(closes,1);assert.equal(calls,1);assert.equal(f.timers.size,0);
});
test('protocol errors kill only this bridge',async()=>{
    const f=fixture();const p=await opened(f);let error;p.on('error',e=>error=e);
    f.child.send({v:99,type:'data',data:'AA=='});await tick();assert(error);assert.equal(f.child.stdin.writableEnded,true);assert.equal(f.timers.size,0);
});
test('close during startup cancels callback and pending writes',async()=>{
    const f=fixture({handshake:false});let opens=0,writes=0;
    const p=new f.Port({path:'COM11',baudRate:9600},e=>{assert(e);opens++;});
    p.write(Buffer.from([1]),e=>{assert(e);writes++;});await tick();await closed(p);await tick();
    assert.equal(opens,1);assert.equal(writes,1);assert.equal(f.timers.size,0);
});
test('oversized writes rejected without transmitting',async()=>{
    const f=fixture();const p=await opened(f);
    await new Promise(resolve=>p.write(Buffer.alloc(1024*1024+1),e=>{assert(e);resolve();}));
    assert(!f.commands.some(c=>c.op==='write'));await closed(p);
});
test('startup timeout kills hung child and leaves no callbacks/timers',async()=>{
    const f=fixture({handshake:false});let errors=0;
    await new Promise(resolve=>{
        const p=new f.Port({path:'COM11',baudRate:9600},e=>{assert.equal(e.code,'ERR_BRIDGE_TIMEOUT');resolve();});
        p.on('error',()=>errors++);
    });
    await tick();assert.equal(errors,0);assert.equal(f.child.stdin.writableEnded,true);assert.equal(f.timers.size,0);
});
test('native close failure reaches close callback once',async()=>{
    const f=fixture({onCommand:(cmd,c)=>{if(cmd.op==='close'){c.send({type:'reply',id:cmd.id,ok:false,phase:'close',win32Code:6});return false;}}});
    const p=await opened(f);let errors=0,closes=0;
    p.on('error',()=>errors++);p.on('close',()=>closes++);
    await new Promise(resolve=>p.close(e=>{assert.equal(e.win32Code,6);resolve();}));
    await tick();assert.equal(errors,1);assert.equal(closes,1);assert.equal(f.timers.size,0);
});

test('unconfirmed exit blocks a second bridge for the same COM', async () => {
    const f = fixture({stuck:true});
    const first = await opened(f);
    first.on('error', () => {});
    f.child.send({v:99,type:'data'});
    const err = await new Promise(resolve => new f.Port({path:'com11',baudRate:2400},resolve));
    assert.equal(err.code,'ERR_BRIDGE_STILL_PRESENT');
    f.child.emit('close',1,null);
    assert.equal(f.timers.size,0);
});
