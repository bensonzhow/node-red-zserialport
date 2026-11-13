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
    const pool=exports.exports({settings:{}, nodes:{registerType(){}}, log:{error(){}}, _:s=>s});
    return {pool,created,loaded};
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
