'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('vm');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
// Expose the private pool only inside this VM; production source remains unchanged.
const source = fs.readFileSync(path.join(__dirname, '../zserial.js'), 'utf8').replace(/}\s*$/, 'return serialPool;\n}');
function fixture() {
    const created = [], loaded = [];
    function port(kind) { return class extends EventEmitter {
        constructor(options) { super();this.options=options;this.kind=kind;created.push(this); }
    }; }
    const exports = {exports:{}};
    vm.runInNewContext(source, {module:exports, Buffer, console, setTimeout, clearTimeout,
        require(name) {
            if(name==='serialport')return {SerialPort:port('serialport')};
            if(name==='./lib/win32-sync-port'){loaded.push(name);return port('win32-sync');}
            return require(name);
        }});
    const types = {};
    const pool=exports.exports({settings:{}, util:{cloneMessage:m=>({...m})}, nodes:{registerType(name, ctor){ types[name]=ctor; }, createNode(){}}, log:{error(){}}, _:s=>s});
    return {pool,created,loaded,types};
}
test('existing configs retain original constructor and options; sync is lazy opt-in',()=>{
    const f=fixture();
    for(const backend of [undefined,'','serialport','unknown']) {
        const config={serialport:'COM'+(f.created.length+1),serialbaud:9600,parity:'EVEN',databits:8,stopbits:1,backend};
        f.pool.get(config);
        const p=f.created.at(-1);assert.equal(p.kind,'serialport');
        assert.equal(p.options.parity,'EVEN');assert.equal(p.options.baudRate,9600);
    }
    assert.equal(f.loaded.length,0);
    f.pool.get({serialport:'COM11',backend:'win32-sync',serialbaud:1200});
    assert.equal(f.created.at(-1).kind,'win32-sync');assert.equal(f.created.at(-1).options.baudRate,1200);
    assert.equal(f.loaded.length,1);
});
test('port pool preserves first configuration until close',()=>{
    const f=fixture();const a=f.pool.get({serialport:'COM11'});
    const b=f.pool.get({serialport:'COM11',backend:'win32-sync'});
    assert.equal(a,b);assert.equal(f.created.length,1);assert.equal(f.loaded.length,0);
});
test('sync close retains the port until exit and coalesces concurrent closes',async()=>{
    const {pool,created}=fixture();
    const config={serialport:'COM15',backend:'win32-sync'};
    const connection=pool.get(config);
    let finish, closes=0;
    created[0].close=cb=>{closes++;finish=cb;};
    const close=()=>new Promise((resolve,reject)=>pool.close('COM15',err=>err?reject(err):resolve()));
    const first=close(), second=close();
    await Promise.resolve();
    assert.equal(pool.curConnections().COM15,connection);
    assert.equal(closes,1);
    finish();
    await Promise.all([first,second]);
    assert.equal(pool.curConnections().COM15,undefined);
    assert.notEqual(pool.get(config),connection);
});
test('repeated closeAll reports both live bridges until their individual exits', async()=>{
    const {pool}=fixture();
    const ports=['COM13','COM17','COM19'];
    const adapters=ports.map(port=>pool.get({serialport:port,backend:'win32-sync'}).serial);
    adapters.forEach((adapter,i)=>{
        adapter.bridgePresent=i<2;
        adapter.close=cb=>cb(adapter.bridgePresent ? new Error(ports[i]+' pid='+i) : null);
    });
    const closeAll=()=>new Promise(resolve=>pool.closeAll(resolve));
    for(let i=0;i<2;i++){
        const err=await closeAll();
        assert.equal(err.errors.length,2);
        assert.deepEqual(Object.keys(pool.curConnections()).sort(),ports.slice(0,2));
    }
    adapters[0].bridgePresent=false; adapters[0].emit('bridgeReleased');
    assert.equal(pool.curConnections().COM13,undefined);
    assert.match((await closeAll()).message,/COM17/);
    adapters[1].bridgePresent=false; adapters[1].emit('bridgeReleased');
    assert.equal(await closeAll(),undefined);
    assert.notEqual(pool.get({serialport:'COM13',backend:'win32-sync'}).serial,adapters[0]);
});

test('one unconfirmed sync exit reaches Catch and healthy positions finish the batch', async () => {
    const { pool, types } = fixture();
    const stuck = Object.assign(new Error('Bridge exit unconfirmed'), {
        backend: 'win32-sync', code: 'ERR_BRIDGE_STILL_PRESENT', port: 'COM21', bridgePid: 7352
    });
    const configs = Array.from({length:12}, (_, i) => ({
        serialport:'COM' + (11+i), backend:'win32-sync', payload:Buffer.from([1])
    }));
    const adapter = pool.get(configs[10]).serial;
    adapter.bridgePresent = true;
    adapter.close = cb => cb(stuck);
    const input = {_msgid:'round1', serialConfigs:configs};
    const init = new EventEmitter();
    const caught = [], forwarded = [];
    init.error = error => caught.push(error);
    init.send = msg => forwarded.push(msg);
    types['zserial closeAll'].call(init, {});
    await new Promise((resolve,reject) => init.emit('input',input,null,err => err ? reject(err) : resolve()));
    assert.equal(caught.length,1);
    assert.equal(forwarded[0],input);

    const connections = pool.curConnections();
    for (const cfg of configs.filter(c => c.serialport !== 'COM21')) {
        const conn = new EventEmitter();
        conn.isopen = true;
        conn.serial = {path:cfg.serialport};
        conn.enqueue = (request,sender) => conn.emit('data',{...request},sender);
        connections[cfg.serialport] = conn;
    }
    const batch = new EventEmitter();
    const summaries = [];
    batch.send = outputs => { if (outputs[2]) summaries.push(outputs[2].payload); };
    types['zserial batch'].call(batch,{});
    await new Promise((resolve,reject) => batch.emit('input',input,null,err => err ? reject(err) : resolve()));
    assert.equal(summaries.length,1);
    assert.equal(Object.keys(summaries[0].successMsg).length,11);
    assert.equal(summaries[0].errorMsg.COM21.status,'ERR_CLOSE');
});

test('persistent sync port survives a flow close, retains requested write baud and closes on shutdown', async () => {
    const {pool} = fixture();
    const conn = pool.get({serialport:'COM17', backend:'win32-sync', syncKeepOpen:true, serialbaud:9600});
    const adapter = conn.serial;
    adapter.isOpen = true;
    const recorded = [];
    adapter.trace = event => recorded.push(event);
    adapter.write = (data, cb, baud) => recorded.push(baud);
    adapter.close = cb => {recorded.push('close'); cb();};
    const close = flowClose => new Promise((resolve,reject) =>
        pool.closeAll(err => err ? reject(err) : resolve(), null, flowClose));
    conn.write(Buffer.from([1]), null, {serialbaud:9600});
    await close(true);
    assert.equal(pool.curConnections().COM17.serial, adapter);
    conn.write(Buffer.from([2]), null, {serialbaud:2400});
    await close(false);
    assert.deepEqual(recorded,[9600,'retain',2400,'close']);
    assert.equal(pool.curConnections().COM17, undefined);
});

function frameFixture(backend) {
    const { pool } = fixture();
    const conn = pool.get({ serialport: 'COM11', backend, out: 'frame' });
    const replies = [];
    conn.serial.write = () => {};
    conn.on('data', (msg, sender) => replies.push({ msg, sender }));
    return { conn, replies };
}

const addressRead = Buffer.from('6852664914000068110434373337CF16', 'hex');
const energyRead = Buffer.from('6852664914000068110433333433C716', 'hex');
const energyReply = Buffer.from('6852664914000068910833333433A9564733C416', 'hex');
const wakeup = Buffer.from('FEFEFEFE', 'hex');

test('645 address read echo leaves the request queued until timeout', async () => {
    const { conn, replies } = frameFixture('win32-sync');
    const sender = {};
    const request = { _msgid: 'address', payload: Buffer.concat([wakeup, addressRead]), timeout: 20 };
    const timeout = new Promise(resolve => conn.once('timeout', (msg, owner) => resolve({ msg, owner })));
    conn.enqueue(request, sender);
    conn.serial.emit('data', addressRead);
    assert.equal(conn.queue.length, 1);
    assert.equal(replies.length, 0);
    const { msg, owner } = await timeout;
    assert.equal(owner, sender);
    assert.equal(msg.request_msgid, request._msgid);
    assert.equal(msg.status, 'ERR_TIMEOUT');
    assert.equal(msg.payload.length, 0);
    assert.equal(conn.queue.length, 0);
});

test('645 energy reply still completes the request when joined to or split after its echo', () => {
    const stream = Buffer.concat([energyRead, wakeup, energyReply]);
    for (const backend of ['serialport', 'win32-sync']) {
        for (let cut = 0; cut < stream.length; cut++) {
            const { conn, replies } = frameFixture(backend);
            const sender = {};
            conn.enqueue({ _msgid: 'energy', payload: energyRead }, sender);
            try {
                if (cut) conn.serial.emit('data', stream.subarray(0, cut));
                assert.equal(replies.length, 0, `early output for ${backend} at byte ${cut}`);
                conn.serial.emit('data', stream.subarray(cut));
                assert.equal(replies.length, 1);
                assert.equal(replies[0].sender, sender);
                const { msg } = replies[0];
                assert.equal(msg.request_msgid, 'energy');
                assert.equal(msg.status, 'OK');
                assert.deepEqual(msg.payload.subarray(-energyReply.length), energyReply);
                assert.equal(msg.frame_meta.proto, '645');
                assert.equal(conn.queue.length, 0);
            } finally {
                clearTimeout(conn.tout);
            }
        }
    }
});

function sumBytes(buf) {
    return buf.reduce((sum, byte) => (sum + byte) & 0xFF, 0);
}

test('645 keeps a checksum-valid false terminator inside the data field', () => {
    const frame = Buffer.from('685266491400006891063437333700161016', 'hex');
    frame[14] = sumBytes(frame.subarray(0, 14));
    frame[16] = sumBytes(frame.subarray(0, 16));
    const { conn, replies } = frameFixture('serialport');
    conn.serial.emit('data', frame.subarray(0, 16));
    assert.equal(replies.length, 0);
    conn.serial.emit('data', frame.subarray(16));
    assert.deepEqual(replies.map(({ msg }) => msg.payload), [frame]);
});

test('645 rejected length and checksum resynchronize without consuming the next reply', () => {
    const invalidLength = Buffer.from(energyReply);
    invalidLength[9] += 3;
    const invalidChecksum = Buffer.from(energyReply);
    invalidChecksum[invalidChecksum.length - 2] ^= 1;
    for (const invalid of [invalidLength, invalidChecksum]) {
        const { conn, replies } = frameFixture('win32-sync');
        const sender = {};
        conn.enqueue({ _msgid: 'energy', payload: energyRead }, sender);
        try {
            conn.serial.emit('data', Buffer.concat([invalid, energyReply.subarray(0, 4)]));
            assert.equal(conn.queue.length, 1);
            conn.serial.emit('data', energyReply.subarray(4));
            const good = replies.filter(({ msg }) => msg.status === 'OK');
            assert.equal(good.length, 1);
            assert.deepEqual(good[0].msg.payload, energyReply);
            assert.equal(good[0].sender, sender);
        } finally {
            clearTimeout(conn.tout);
        }
    }
});

test('unrelated 645 frames do not complete a request or consume the next queued request', () => {
    const { conn, replies } = frameFixture('serialport');
    const sender = {};
    conn.enqueue({ _msgid: 'energy', payload: energyRead }, sender);
    conn.enqueue({ _msgid: 'next', payload: energyRead }, {});
    try {
        const otherDI = Buffer.from(energyReply);
        otherDI[10]++;
        otherDI[otherDI.length - 2] = sumBytes(otherDI.subarray(0, -2));
        conn.serial.emit('data', Buffer.concat([addressRead, otherDI, energyReply, energyReply]));
        assert.deepEqual(replies.map(({ msg, sender }) => [msg.request_msgid, sender]), [
            [undefined, null], [undefined, null], ['energy', sender], [undefined, null]
        ]);
        assert.equal(conn.queue[0].msg._msgid, 'next');
    } finally {
        clearTimeout(conn.tout);
    }
});

test('a recognized partial 645 reply can complete after the initial response deadline', async () => {
    const { conn, replies } = frameFixture('win32-sync');
    const timeouts = [];
    conn.on('timeout', msg => timeouts.push(msg));
    conn.enqueue({ _msgid: 'energy', payload: energyRead, timeout: 15, maxPartialWait: 500 }, {});
    try {
        conn.serial.emit('data', energyReply.subarray(0, 12));
        await new Promise(resolve => setTimeout(resolve, 35));
        assert.equal(timeouts.length, 0);
        assert.equal(replies.length, 0);
        conn.serial.emit('data', energyReply.subarray(12));
        assert.equal(replies[0].msg.request_msgid, 'energy');
    } finally {
        clearTimeout(conn.tout);
    }
});

function frameCrc(buf) {
    let crc = 0xFFFF;
    for (const byte of buf) {
        crc ^= byte;
        for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0x8408 : 0);
    }
    return crc ^ 0xFFFF;
}

test('698 uses HCS to keep 2017 and draft frames intact, including frames over 2048 bytes', () => {
    const legacyHeader = Buffer.from('680000430552666814000000', 'hex');
    const draftHeader = Buffer.from('68000004000100', 'hex');
    const legacy = Buffer.concat([legacyHeader, Buffer.alloc(2200), Buffer.from([0, 0, 0x16])]);
    const draft = Buffer.concat([draftHeader, Buffer.alloc(8), Buffer.from([0, 0, 0x16])]);
    for (const [frame, header] of [[legacy, legacyHeader], [draft, draftHeader]]) {
        frame.writeUInt16LE(frame.length - 2, 1);
        frame.writeUInt16LE(frameCrc(frame.subarray(1, header.length)), header.length);
        frame.writeUInt16LE(frameCrc(frame.subarray(1, -3)), frame.length - 3);
        for (let cut = 1; cut <= header.length + 2; cut++) {
            const { conn, replies } = frameFixture('serialport');
            conn.serial.emit('data', frame.subarray(0, cut));
            assert.equal(replies.length, 0);
            conn.serial.emit('data', Buffer.concat([frame.subarray(cut), energyReply]));
            assert.deepEqual(replies.map(({ msg }) => msg.payload), [frame, energyReply]);
            assert.equal(replies[0].msg.frame_meta.proto, '698-len');
            assert.equal(replies[0].msg.frame_meta.fcs_ok, true);
        }
    }
});

test('GNW BLE uses its length even when the data contains 7E A5', () => {
    const frame = Buffer.from('7E7E7E5A0A817EA56816007EA5', 'hex');
    frame[frame.length - 3] = sumBytes(frame.subarray(0, -3));
    const resetReply = Buffer.from('7E7E7E5A0780005B7EA5', 'hex');
    for (let cut = 1; cut < frame.length; cut++) {
        const { conn, replies } = frameFixture('serialport');
        conn.serial.emit('data', frame.subarray(0, cut));
        assert.equal(replies.length, 0);
        conn.serial.emit('data', Buffer.concat([frame.subarray(cut), resetReply]));
        assert.deepEqual(replies.map(({ msg }) => msg.payload), [frame, resetReply]);
        assert.equal(replies[0].msg.frame_meta.proto, 'ble-gnw');
    }
});
