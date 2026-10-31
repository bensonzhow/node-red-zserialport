'use strict';

const { EventEmitter } = require('events');
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

const VERSION = 1;
const MAX_WRITE = 1024 * 1024;
const MAX_LINE = 2 * 1024 * 1024;
const MAX_PENDING = 256;

function failure(message, phase, code) {
    return Object.assign(new Error(message), { backend: 'win32-sync', phase, code });
}

class Win32SyncPort extends EventEmitter {
    constructor(options, callback) {
        super();
        this.path = options.path;
        this.isOpen = false;
        this._options = { ...options };
        this._pending = new Map();
        this._nextId = 0;
        this._queuedBytes = 0;
        this._state = 'new';
        this._closedEmitted = false;
        this._stdout = '';
        this._stderr = '';
        this._beforeOpen = [];
        this._beforeOpenBytes = 0;
        if (options.autoOpen !== false) process.nextTick(() => this.open(callback));
    }

    open(callback) {
        if (this._state !== 'new') return this._report(failure('Port already opened or closed', 'open'), callback);
        this._state = 'opening';
        this._openCallback = callback;
        try {
            if (process.platform !== 'win32') throw failure('win32-sync requires Windows', 'open', 'ERR_BACKEND_UNAVAILABLE');
            const arch = { ia32: 'x86', x64: 'x64' }[process.arch];
            if (!arch) throw failure('Unsupported Windows architecture: ' + process.arch, 'open', 'ERR_BACKEND_UNAVAILABLE');
            if (!/^COM[0-9]+$/i.test(this.path)) throw failure('Expected a Windows COM port name', 'open');
            const o = this._options;
            this._wireOptions = {
                port: this.path.toUpperCase(), baud: Number(o.baudRate),
                bits: Number(o.dataBits === undefined ? 8 : o.dataBits),
                stops: Number(o.stopBits === undefined ? 1 : o.stopBits),
                parity: String(o.parity || 'none').toLowerCase()
            };
            const w = this._wireOptions;
            if (!Number.isInteger(w.baud) || w.baud <= 0 || w.baud > 0xffffffff ||
                ![5, 6, 7, 8].includes(w.bits) || ![1, 1.5, 2].includes(w.stops) ||
                !['none', 'even', 'odd', 'mark', 'space'].includes(w.parity)) throw failure('Invalid serial configuration', 'open');
            const exe = path.join(__dirname, '..', 'bin', 'windows', arch, 'zserial-sync.exe');
            fs.accessSync(exe, fs.constants.R_OK);
            const child = this._child = spawn(exe, [String(process.pid)], {
                windowsHide: true, shell: false, stdio: ['pipe', 'pipe', 'pipe']
            });
            this._startupTimer = setTimeout(() => this._abort(failure('Bridge startup/open timed out', 'open', 'ERR_BRIDGE_TIMEOUT')), 5000);
            child.stdout.setEncoding('utf8');
            child.stderr.setEncoding('utf8');
            child.stdout.on('data', chunk => this._consume(chunk));
            child.stderr.on('data', chunk => { this._stderr = (this._stderr + chunk).slice(-8192); });
            child.stdin.on('error', err => this._abort(Object.assign(err, { phase: 'ipc' })));
            child.on('error', err => this._abort(Object.assign(err, { phase: 'spawn' })));
            child.on('close', (code, signal) => this._onExit(code, signal));
        } catch (err) { this._abort(err); }
    }

    _consume(chunk) {
        if (this._state === 'closed' || this._state === 'failed') return;
        this._stdout += chunk;
        if (Buffer.byteLength(this._stdout) > MAX_LINE) return this._abort(failure('Bridge output exceeded limit', 'ipc'));
        let index;
        while ((index = this._stdout.indexOf('\n')) >= 0) {
            const line = this._stdout.slice(0, index);
            this._stdout = this._stdout.slice(index + 1);
            try { this._message(JSON.parse(line)); }
            catch (err) { this._abort(Object.assign(err, { phase: 'ipc' })); return; }
        }
    }

    _message(m) {
        if (m.v !== VERSION) throw failure('Incompatible bridge protocol version', 'ipc');
        if (m.type === 'hello') {
            if (this._hello || this._state !== 'opening' || m.bits !== (process.arch === 'ia32' ? 32 : 64)) throw failure('Unexpected bridge handshake', 'ipc');
            this._hello = true;
            this._bridgeVersion = m.build;
            this._request('open', this._wireOptions, err => {
                clearTimeout(this._startupTimer);
                if (err) return this._abort(err);
                if (this._state !== 'opening') return;
                this._state = 'open'; this.isOpen = true;
                const cb = this._openCallback; this._openCallback = null;
                this.emit('open');
                if (cb) cb(null);
                const writes = this._beforeOpen; this._beforeOpen = []; this._beforeOpenBytes = 0;
                for (const item of writes) this.write(item.data, item.callback);
            });
            return;
        }
        if (m.type === 'data') {
            if (this._state !== 'open') return;
            if (typeof m.data !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(m.data)) throw failure('Invalid RX encoding', 'ipc');
            const data = Buffer.from(m.data, 'base64');
            if (data.length) this.emit('data', data);
            return;
        }
        if (m.type === 'fatal') {
            this._abort(this._remoteError(m)); return;
        }
        if (m.type !== 'reply' || !Number.isSafeInteger(m.id) || typeof m.ok !== 'boolean') throw failure('Invalid bridge reply', 'ipc');
        const pending = this._pending.get(m.id);
        if (!pending) throw failure('Unexpected bridge reply id', 'ipc');
        this._pending.delete(m.id); clearTimeout(pending.timer);
        this._queuedBytes -= pending.bytes;
        if (m.ok && pending.op === 'write' && m.written !== pending.bytes) {
            const err = failure('Bridge reported incomplete write', 'write'); err.writtenBytes = m.written;
            pending.callback(err); return;
        }
        pending.callback(m.ok ? null : this._remoteError(m));
    }

    _remoteError(m) {
        return Object.assign(failure(m.message || 'Native serial operation failed', m.phase, 'ERR_NATIVE'), {
            win32Code: m.win32Code, writtenBytes: m.written, bridgeVersion: this._bridgeVersion
        });
    }

    _request(op, args, callback, bytes = 0) {
        if (!this._child || !this._child.stdin.writable || ['closed', 'failed'].includes(this._state)) {
            process.nextTick(() => callback(failure('Bridge is not available', op))); return;
        }
        if (this._pending.size >= MAX_PENDING || this._queuedBytes + bytes > 4 * MAX_WRITE || this._child.stdin.writableLength > 6 * MAX_WRITE) {
            process.nextTick(() => callback(failure('Bridge queue limit exceeded', op, 'ERR_BACKPRESSURE'))); return;
        }
        const id = ++this._nextId;
        const timer = setTimeout(() => this._abort(failure(op + ' operation timed out', op, 'ERR_BRIDGE_TIMEOUT')), 5000);
        this._pending.set(id, { op, callback, timer, bytes }); this._queuedBytes += bytes;
        this._child.stdin.write(JSON.stringify({ v: VERSION, id, op, ...args }) + '\n');
    }

    _report(err, callback) {
        process.nextTick(() => { if (callback) callback(err); else this.emit('error', err); });
    }

    _operation(op, args, callback, bytes) {
        if (!this.isOpen) return this._report(failure('Port is not open', op), callback);
        this._request(op, args, err => { if (callback) callback(err); else if (err) this.emit('error', err); }, bytes);
    }

    write(data, callback) {
        const b = Buffer.isBuffer(data) ? data : Buffer.from(data);
        if (b.length > MAX_WRITE) { this._report(failure('Write exceeds 1MiB limit', 'write'), callback); return false; }
        if (this._state === 'opening' || this._state === 'new') {
            if (this._beforeOpen.length >= MAX_PENDING || this._beforeOpenBytes + b.length > 4 * MAX_WRITE) {
                this._report(failure('Pre-open queue limit exceeded', 'write', 'ERR_BACKPRESSURE'), callback); return false;
            }
            this._beforeOpen.push({ data: Buffer.from(b), callback }); this._beforeOpenBytes += b.length;
            return true;
        }
        this._operation('write', { data: b.toString('base64') }, callback, b.length);
        return this._queuedBytes < 4 * MAX_WRITE;
    }
    update(options, callback) {
        const baud = Number(options.baudRate);
        if (!Number.isInteger(baud) || baud <= 0 || baud > 0xffffffff) return this._report(failure('Invalid baud rate', 'update'), callback);
        this._operation('update', { baud }, callback);
    }
    set(flags = {}, callback) {
        // Match serialport's defaults and Windows CTS/DSR event-mask behavior.
        this._operation('set', { rts: flags.rts === undefined ? true : !!flags.rts,
            dtr: flags.dtr === undefined ? true : !!flags.dtr, brk: !!flags.brk,
            cts: !!flags.cts, dsr: !!flags.dsr }, callback);
    }
    flush(callback) { this._operation('flush', {}, callback); }
    close(callback) {
        if (this._state === 'closed') { if (callback) process.nextTick(() => callback(null)); return; }
        if (!this._closeCallbacks) this._closeCallbacks = [];
        if (callback) this._closeCallbacks.push(callback);
        if (this._state === 'closing') return;
        this._state = 'closing'; this.isOpen = false;
        clearTimeout(this._startupTimer);
        if (this._openCallback) { const cb = this._openCallback; this._openCallback = null; process.nextTick(() => cb(failure('Open canceled by close', 'open'))); }
        this._failBeforeOpen(failure('Port closed before write', 'write'));
        this._request('close', {}, err => { if (err) this._abort(err); });
        this._closeTimer = setTimeout(() => this._abort(failure('Bridge close timed out', 'close')), 5000);
        if (!this._child) this._onExit(null, null);
    }

    _abort(err) {
        if (this._state === 'failed' || this._state === 'closed') return;
        this._terminalError = Object.assign(err, { backend: 'win32-sync', stderr: this._stderr, bridgeVersion: this._bridgeVersion });
        const opening = this._state === 'opening';
        this._suppressClose = opening;
        this._state = 'failed'; this.isOpen = false;
        clearTimeout(this._startupTimer); clearTimeout(this._closeTimer);
        const cb = this._openCallback; this._openCallback = null;
        this._failPending(err);
        this._failBeforeOpen(err);
        if (this._child) this._child.kill();
        if (opening && cb) cb(err); else if (!opening || !cb) this.emit('error', err);
        // Initial open failures follow serialport: callback only, no close event.
        if (!this._child) this._onExit(null, null);
    }

    _failPending(err) {
        const pending = [...this._pending.values()]; this._pending.clear(); this._queuedBytes = 0;
        for (const item of pending) { clearTimeout(item.timer); item.callback(err); }
    }

    _failBeforeOpen(err) {
        const writes = this._beforeOpen; this._beforeOpen = []; this._beforeOpenBytes = 0;
        for (const item of writes) this._report(err, item.callback);
    }

    _onExit(code, signal) {
        if (this._closedEmitted) return;
        if (!['closing', 'failed'].includes(this._state)) this._abort(failure('Bridge exited: ' + code + ' ' + signal, 'exit', 'ERR_BRIDGE_EXIT'));
        clearTimeout(this._startupTimer); clearTimeout(this._closeTimer);
        this.isOpen = false; this._state = 'closed'; this._closedEmitted = true;
        this._failPending(this._terminalError || failure('Port closed', 'close'));
        if (!this._suppressClose) this.emit('close');
        for (const cb of this._closeCallbacks || []) cb(this._terminalError || null);
        this._closeCallbacks = [];
    }
}

module.exports = Win32SyncPort;
