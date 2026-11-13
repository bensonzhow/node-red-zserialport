'use strict';

// 在加载串口原生依赖前启用日志，依赖加载失败也能留下错误信息。
// 同步追加避免超时 process.exit() 或 Ctrl+C 时丢失已输出的数据。
function enableFileLogging() {
    const fs = require('fs');
    const path = require('path');
    const filename = path.join(process.cwd(), `serial-test-${new Date().toISOString().replace(/[:.]/g, '-')}-${process.pid}.log`);
    try {
        fs.writeFileSync(filename, '\uFEFF', { encoding: 'utf8', flag: 'wx' });
    } catch (err) {
        console.error('无法创建日志文件，将继续仅输出到屏幕：', filename, err);
        return;
    }
    let failed = false;
    const originalStderr = process.stderr.write.bind(process.stderr);
    for (const stream of [process.stdout, process.stderr]) {
        const originalWrite = stream.write.bind(stream);
        stream.write = function (chunk, encoding, callback) {
            if (!failed) {
                try {
                    fs.appendFileSync(filename, typeof chunk === 'string'
                        ? Buffer.from(chunk, typeof encoding === 'string' ? encoding : 'utf8') : chunk);
                } catch (err) {
                    failed = true;
                    originalStderr(`日志写入失败，后续仅显示在屏幕：${err.message}\n`);
                }
            }
            return originalWrite(chunk, encoding, callback);
        };
    }
    console.log('日志文件（UTF-8）：' + filename);
    console.log('启动参数：' + JSON.stringify(process.argv));
    process.on('exit', code => {
        console.log(`[${new Date().toISOString()}] PROCESS EXIT code=${code}`);
    });
}

if (require.main === module) enableFileLogging();

// 可直接修改默认配置，也可用命令行参数覆盖。默认自动探测：先 698，失败后 645；--hex/--text 为自定义发送模式。
const CONFIG = {
    port: 'COM11',
    backend: 'serialport',
    baudRate: 9600,
    dataBits: 8,
    stopBits: 1,
    parity: 'even',
    hex: '', // 示例：'68 01 02 03 16'；填写实际设备报文
    text: '', // UTF-8 文本，与 hex 二选一；代码中可填写 '\r\n'
    openTimeout: 10000,
    openDelay: 0,
    rts: 'auto',
    dtr: 'auto',
    skipDrain: false,
    skipRegistry: false,
    receiveTimeout: 5000 // 发送完成后监听时长；0 表示持续监听
};

const { SerialPort } = require('serialport');
const { inspect } = require('util');
const { execFile } = require('child_process');
const { promisify } = require('util');
const execFileAsync = promisify(execFile);
const options = { showHidden: true, depth: null, colors: false,
    maxArrayLength: null, maxStringLength: null };

function log(label, value) {
    console.log(`[${new Date().toISOString()}] ${label}`, value === undefined ? '' : value);
}

function error(phase, err) {
    log(`ERROR ${phase}`, inspect(err, options));
}

function packet(label, data) {
    log(`${label} ${data.length} bytes`, `HEX=${data.toString('hex').toUpperCase().match(/.{2}/g)?.join(' ') || ''}`);
    log(`${label} UTF8`, JSON.stringify(data.toString('utf8')));
}

function parseArgs(args) {
    const config = { ...CONFIG };
    const numbers = { '--baud': 'baudRate', '--data-bits': 'dataBits',
        '--stop-bits': 'stopBits', '--open-timeout': 'openTimeout', '--timeout': 'receiveTimeout', '--open-delay': 'openDelay' };
    const strings = { '--backend': 'backend', '--port': 'port', '--parity': 'parity', '--hex': 'hex', '--text': 'text', '--rts': 'rts', '--dtr': 'dtr' };
    let payloadOption;
    for (let i = 0; i < args.length; i++) {
        const key = args[i];
        if (key === '--skip-drain') { config.skipDrain = true; continue; }
        if (key === '--skip-registry') { config.skipRegistry = true; continue; }
        if (key === '--list') { config.listOnly = true; continue; }
        if (key === '--help' || key === '-h') { config.help = true; continue; }
        if (!numbers[key] && !strings[key]) throw new Error(`未知参数：${key}`);
        if (args[i + 1] === undefined) throw new Error(`${key} 缺少参数值`);
        const value = args[++i];
        if (key === '--hex' || key === '--text') {
            if (payloadOption && payloadOption !== key) throw new Error('--hex 与 --text 不能同时使用');
            payloadOption = key;
            config.hex = '';
            config.text = '';
        }
        config[numbers[key] || strings[key]] = numbers[key] ? Number(value) : value;
    }
    if (config.help || config.listOnly) return config;
    if (!['serialport', 'win32-sync'].includes(config.backend)) throw new Error('backend 必须为 serialport/win32-sync');
    if (config.backend === 'win32-sync') config.skipDrain = true;
    if (!config.port.trim()) throw new Error('串口路径不能为空');
    if (!Number.isInteger(config.baudRate) || config.baudRate <= 0) throw new Error('波特率必须为正整数');
    if (![5, 6, 7, 8].includes(config.dataBits)) throw new Error('数据位必须为 5/6/7/8');
    if (![1, 1.5, 2].includes(config.stopBits)) throw new Error('停止位必须为 1/1.5/2');
    if (!['none', 'even', 'odd', 'mark', 'space'].includes(config.parity)) throw new Error('校验位必须为 none/even/odd/mark/space');
    for (const key of ['rts', 'dtr']) {
        if (!['auto', 'on', 'off'].includes(config[key])) throw new Error(key + ' 必须为 auto/on/off');
    }
    if ((config.rts === 'auto') !== (config.dtr === 'auto')) throw new Error('--rts 与 --dtr 请同时指定 on/off，避免 set() 默认值影响对照');
    for (const key of ['openTimeout', 'receiveTimeout', 'openDelay']) {
        if (!Number.isInteger(config[key]) || config[key] < (key === 'openTimeout' ? 1 : 0) || config[key] > 2147483647) {
            throw new Error(`${key} 必须为有效的毫秒整数（openTimeout > 0，receiveTimeout >= 0）`);
        }
    }
    if (config.hex && config.text) throw new Error('CONFIG.hex 与 CONFIG.text 请只填写一个');
    const hex = config.hex.replace(/\s/g, '');
    if (hex && (!/^[0-9a-f]+$/i.test(hex) || hex.length % 2)) throw new Error('HEX 报文必须为完整字节，例如 "68 01 16"，不支持 0x 前缀');
    config.autoProbe = !config.hex && !config.text;
    if (config.autoProbe && config.receiveTimeout === 0) throw new Error('自动探测需要 --timeout > 0，以便切换到 645');
    config.payload = hex ? Buffer.from(hex, 'hex') : Buffer.from(config.text, 'utf8');
    return config;
}

const PROBES = [
    { protocol: '698', hex: 'FEFEFEFE6817004345AAAAAAAAAAAA11534E0501004001020000ED0316' },
    { protocol: '645', hex: 'FEFEFEFE68AAAAAAAAAAAA681300DF16' }
];

function crc16(data) {
    let crc = 0xffff;
    for (const byte of data) {
        crc ^= byte;
        for (let i = 0; i < 8; i++) crc = (crc >>> 1) ^ ((crc & 1) ? 0x8408 : 0);
    }
    return (crc ^ 0xffff) & 0xffff;
}

// 扫描所有候选起点：兼容前导 FE、噪声、分包及粘包；请求回显不能作为响应。
// 仅识别本次读地址请求的正常响应，不是通用的 698 APDU 解码器。
function parseResponse(buffer, protocol) {
    let reason = '尚无完整的有效响应帧';
    for (let offset = 0; offset < buffer.length; offset++) {
        if (buffer[offset] !== 0x68) continue;
        const b = buffer.subarray(offset);
        if (protocol === '645') {
            if (b.length < 10 || b[7] !== 0x68) continue;
            const size = 12 + b[9];
            if (b.length < size) continue;
            const f = b.subarray(0, size);
            if (f[size - 1] !== 0x16) { reason = '645 结束符错误'; continue; }
            const cs = f.subarray(0, size - 2).reduce((sum, byte) => (sum + byte) & 255, 0);
            if (cs !== f[size - 2]) { reason = '645 CS 校验失败'; continue; }
            if (f[8] !== 0x93 || f[9] !== 6) { reason = '不是 645 读地址正常响应（可能为回显或异常响应）'; continue; }
            const address = Buffer.from(f.subarray(10, 16).map(byte => (byte - 0x33) & 255)).reverse().toString('hex').toUpperCase();
            if (!/^[0-9]{12}$/.test(address)) { reason = '645 地址不是有效 BCD'; continue; }
            return { ok: true, protocol, address, frame: f.toString('hex').toUpperCase() };
        }
        if (b.length < 5) continue;
        const raw = b.readUInt16LE(1);
        const size = (raw & 0x3fff) * ((raw & 0x4000) ? 1024 : 1) + 2;
        if ((raw & 0x8000) || size < 12 || size > 65536) { reason = '698 长度域无效或超出诊断上限'; continue; }
        if (b.length < size) continue;
        const f = b.subarray(0, size);
        const addressLength = (f[4] & 15) + 1;
        const hcsOffset = 6 + addressLength;
        const apduOffset = hcsOffset + 2;
        if (apduOffset > size - 3 || f[size - 1] !== 0x16) { reason = '698 帧结构错误'; continue; }
        if (crc16(f.subarray(1, hcsOffset)) !== f.readUInt16LE(hcsOffset)) { reason = '698 HCS 校验失败'; continue; }
        if (crc16(f.subarray(1, size - 3)) !== f.readUInt16LE(size - 3)) { reason = '698 FCS 校验失败'; continue; }
        const apdu = f.subarray(apduOffset, size - 3);
        if (!(f[3] & 0x80) || apdu.length < 10 || apdu[0] !== 0x85 || apdu[1] !== 1 ||
            (apdu[2] & 0x3f) !== 0 || apdu.subarray(3, 7).toString('hex') !== '40010200') {
            reason = '不是本次 698 读取 40010200 的正常响应（可能为回显或其他帧）'; continue;
        }
        if (apdu[7] !== 1) { reason = '698 返回 DAR 错误，未返回地址数据'; continue; }
        // 通信地址类型为 octet-string，保留字节顺序，不套用 645 的减 33/倒序规则。
        if (apdu[8] !== 9 || apdu[9] === 0 || apdu[9] > 16 || apdu.length < 10 + apdu[9]) {
            reason = '698 地址数据类型或长度无法解析'; continue;
        }
        return { ok: true, protocol, address: apdu.subarray(10, 10 + apdu[9]).toString('hex').toUpperCase(),
            frame: f.toString('hex').toUpperCase(), apdu: apdu.toString('hex').toUpperCase() };
    }
    return { ok: false, reason };
}

function parseRegistryPorts(stdout) {
    return stdout.replace(/^\uFEFF/, '').split(/\r?\n/).filter(line => line.trim()).map(line => {
        const fields = line.split('\t');
        if (fields.length !== 2 || !/^COM\d+$/i.test(fields[1].trim())) {
            throw new Error('无法解析 SERIALCOMM 记录：' + line);
        }
        return { device: fields[0], path: fields[1].trim().toUpperCase() };
    });
}

function comparePorts(ports, registryPorts) {
    const listed = new Set(ports.map(p => p.path.toUpperCase()));
    const registered = new Set(registryPorts.map(p => p.path.toUpperCase()));
    return {
        both: [...listed].filter(p => registered.has(p)),
        serialportOnly: [...listed].filter(p => !registered.has(p)),
        registryOnly: [...registered].filter(p => !listed.has(p))
    };
}

function isUnsupportedWindowsDrain(err, platform = process.platform) {
    return platform === 'win32' && !!err &&
        /Draining connection \(FlushFileBuffers\): Unknown error code 50\b/.test(err.message || '');
}

async function listRegistryPorts() {
    // 使用固定脚本、无 shell 插值；兼容旧 PowerShell，无需 ConvertTo-Json。
    const script = String.raw`
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding
try {
    $ports = Get-ItemProperty -Path 'HKLM:\HARDWARE\DEVICEMAP\SERIALCOMM'
    foreach ($p in $ports.PSObject.Properties) {
        if ($p.Name -notmatch '^PS(Path|ParentPath|ChildName|Drive|Provider)$') {
            [Console]::WriteLine($p.Name + [char]9 + [string]$p.Value)
        }
    }
    [Console]::Out.Flush()
    exit 0
} catch {
    [Console]::Error.WriteLine($_.Exception.ToString())
    exit 1
}`;
    const { stdout, stderr } = await execFileAsync('powershell.exe', [
        '-NoProfile', '-NonInteractive', '-EncodedCommand', Buffer.from(script, 'utf16le').toString('base64')
    ], { windowsHide: true, timeout: 10000, maxBuffer: 1024 * 1024, encoding: 'utf8' });
    if (stderr.trim()) log('REGISTRY STDERR', stderr.trim());
    return parseRegistryPorts(stdout);
}

async function main() {
    const config = parseArgs(process.argv.slice(2));
    if (config.help) {
        console.log(`串口独立诊断（在此项目目录运行）
  node test.js --list
  node test.js --port COM11 --baud 9600 --hex "68 01 02 03 16"
  node test.js --port COM11 --baud 9600 --text "hello" --timeout 10000
  node test.js --port COM11 --baud 9600 --parity even --timeout 5000

参数：--backend serialport/win32-sync（同步模式自动跳过 drain）
      --port --baud --data-bits --stop-bits --parity
      --hex / --text（不自动追加换行）
      --skip-drain 完全跳过 FlushFileBuffers，只等待 write 回调
      --open-delay 打开并设置控制线后等待毫秒，默认 0
      --rts on/off --dtr on/off 成对指定控制线，默认不额外设置
      --skip-registry 跳过注册表查询，便于重复对照
      --open-timeout 打开超时毫秒，默认 10000
      --timeout 每种协议的响应等待毫秒，默认 5000；自定义发送模式可用 0 持续监听
      --list 仅列出端口；Windows 同时查询 SERIALCOMM 注册表并比较，不打开、不发送
默认依次发送 698、645 读地址报文；698 成功则不再发送 645。
--hex/--text 覆盖自动探测，只发送自定义报文。
接收日志按数据块打印；自动模式会组帧、校验并解析读地址响应。`);
        return;
    }
    log('运行环境', inspect({ node: process.version, platform: process.platform, arch: process.arch }, options));
    let ports;
    let listFailed = false;
    try {
        ports = await SerialPort.list();
        log(`系统枚举端口：${ports.length} 个`, inspect(ports, options));
        if (!config.listOnly && !ports.some(p => p.path.toLowerCase() === config.port.toLowerCase())) {
            log('提示', `${config.port} 未在枚举列表中；仍尝试直接打开，虚拟串口可能未被枚举。`);
        }
    } catch (err) {
        error('LIST', err);
        listFailed = true;
        if (!config.listOnly) log('提示', '枚举失败，继续尝试直接打开指定端口');
    }
    if (process.platform === 'win32' && !config.skipRegistry) {
        log('注册表查询', 'Get-ItemProperty -Path "HKLM:\\HARDWARE\\DEVICEMAP\\SERIALCOMM"');
        try {
            const registryPorts = await listRegistryPorts();
            log(`SERIALCOMM 映射：${registryPorts.length} 条`, inspect(registryPorts, options));
            if (ports) log('端口对比（both=共有，serialportOnly=仅枚举，registryOnly=仅注册表）', inspect(comparePorts(ports, registryPorts), options));
            else log('端口对比', 'SerialPort.list() 失败，无法比较；不将失败视为空列表');
            log('提示', '枚举或注册表中存在端口，不代表能成功打开或通信；列表不同也不阻止直接打开。');
        } catch (err) {
            error('REGISTRY', err);
            listFailed = true;
            if (!config.listOnly) log('提示', '注册表查询失败，继续测试指定串口');
        }
    } else log('注册表查询', '非 Windows 或指定 --skip-registry，跳过 SERIALCOMM 查询');
    if (config.listOnly) { if (listFailed) process.exitCode = 1; return; }

    const serialOptions = { path: config.port, baudRate: config.baudRate,
        dataBits: config.dataBits, stopBits: config.stopBits, parity: config.parity, autoOpen: false };
    log('兼容测试配置', inspect({skipDrain: config.skipDrain, openDelay: config.openDelay, rts: config.rts, dtr: config.dtr}, options));
    log('打开配置', inspect(serialOptions, options));
    log('实际测试后端', config.backend);
    const PortImpl = config.backend === 'win32-sync' ? require('./lib/win32-sync-port') : SerialPort;
    const port = new PortImpl(serialOptions);
    let rawReceived = Buffer.alloc(0);
    let stopping = false;
    let receiveTimer;
    let openTimer;
    let closeTimer;
    let sendTimer;
    let prepareTimer;
    let received = 0;
    let probeIndex = -1;
    let probeBuffer = Buffer.alloc(0);

    function finish(code) {
        if (stopping) return;
        stopping = true;
        process.exitCode = code;
        clearTimeout(openTimer);
        clearTimeout(receiveTimer);
        clearTimeout(sendTimer);
        clearTimeout(prepareTimer);
        log('接收总字节数', received);
        packet('RX 汇总（最多64KiB）', rawReceived);
        if (!config.autoProbe) log('645解析结果', inspect(parseResponse(rawReceived, '645'), options));
        if (config.backend === 'win32-sync') log('同步后端诊断', inspect({bridgeVersion: port._bridgeVersion, configuration: port._wireOptions, stderr: port._stderr}, options));
        if (port.isOpen) {
            closeTimer = setTimeout(() => { error('CLOSE TIMEOUT', new Error('驱动关闭超时')); process.exit(1); }, 3000);
            port.close(err => {
                clearTimeout(closeTimer);
                if (err) { error('CLOSE', err); process.exitCode = 1; }
            });
        }
    }

    function listen() {
        if (stopping) return;
        if (config.receiveTimeout === 0) { log('监听', '持续接收，按 Ctrl+C 退出'); return; }
        receiveTimer = setTimeout(() => {
            log('监听结束', received ? '已收到原始字节；未做协议校验' : '未收到数据；这不等于端口打开失败');
            finish(0);
        }, config.receiveTimeout);
    }

    function send(payload, onSent) {
        packet('TX REQUEST', payload);
        sendTimer = setTimeout(() => { error('SEND TIMEOUT', new Error('写入或 drain 超过 10 秒')); finish(1); }, 10000);
        port.write(payload, err => {
            if (stopping) return;
            if (err) { error('WRITE', err); finish(1); return; }
            log('WRITE OK', '写入回调成功，尚未确认远端收到');
            if (config.skipDrain) {
                clearTimeout(sendTimer);
                log('DRAIN SKIPPED', '未调用 FlushFileBuffers；开始等待回包，不保证物理发送已完成');
                onSent();
                return;
            }
            port.drain(err => {
                clearTimeout(sendTimer);
                if (stopping) return;
                if (err) {
                    if (isUnsupportedWindowsDrain(err)) {
                        error('DRAIN UNSUPPORTED (继续监听)', err);
                        log('兼容处理', '驱动不支持 FlushFileBuffers；不重发报文、不关闭端口，继续等待响应。实际发送完成状态未知。');
                        onSent();
                        return;
                    }
                    error('DRAIN', err); finish(1); return;
                }
                log('TX COMPLETE', '驱动已完成发送；不代表远端设备已确认');
                onSent();
            });
        });
    }

    function nextProbe() {
        if (stopping) return;
        probeIndex++;
        probeBuffer = Buffer.alloc(0);
        const probe = PROBES[probeIndex];
        log('探测协议', probe.protocol);
        send(Buffer.from(probe.hex, 'hex'), () => {
            receiveTimer = setTimeout(() => {
                const result = parseResponse(probeBuffer, probe.protocol);
                log(`${probe.protocol} 探测失败`, probeBuffer.length ? result.reason : '没有收到返回数据');
                if (probeBuffer.length) packet(`${probe.protocol} 未解析数据`, probeBuffer);
                if (probeIndex === 0) { log('切换协议', '发送 645 读地址报文'); nextProbe(); }
                else { log('探测结果', '698 和 645 均未获得可解析的读地址响应'); finish(2); }
            }, config.receiveTimeout);
        });
    }

    port.on('data', data => {
        received += data.length;
        rawReceived = Buffer.concat([rawReceived, data]).subarray(-65536);
        packet('RX', data);
        if (!config.autoProbe || probeIndex < 0 || stopping) return;
        // 限制异常连续流占用；每次扫描均保留最近 64 KiB。
        probeBuffer = Buffer.concat([probeBuffer, data]).subarray(-65536);
        const result = parseResponse(probeBuffer, PROBES[probeIndex].protocol);
        if (result.ok) { log('探测成功', inspect(result, options)); finish(0); }
    });
    port.on('error', err => { error('SERIAL EVENT', err); finish(1); });
    port.on('close', () => {
        log('CLOSED', config.port);
        if (!stopping) { error('UNEXPECTED CLOSE', new Error('串口意外关闭')); finish(1); }
    });
    process.once('SIGINT', () => {
        if (port.isOpen) finish(0);
        else process.exit(130);
    });
    openTimer = setTimeout(() => {
        error('OPEN TIMEOUT', new Error(`${config.port} 打开超过 ${config.openTimeout}ms，驱动未返回`));
        // 某些驱动会一直挂起打开请求，直接退出独立诊断进程以释放资源。
        process.exit(1);
    }, config.openTimeout);
    port.open(err => {
        clearTimeout(openTimer);
        if (err) { error(`OPEN ${config.port}`, err); finish(1); return; }
        if (stopping) { port.close(); return; }
        log('OPEN OK', config.port);
        const start = () => {
            if (stopping) return;
            log('发送前等待毫秒', config.openDelay);
            prepareTimer = setTimeout(() => {
                if (stopping) return;
                if (config.autoProbe) nextProbe();
                else send(config.payload, listen);
            }, config.openDelay);
        };
        if (config.rts === 'auto') { start(); return; }
        const flags = {rts: config.rts === 'on', dtr: config.dtr === 'on'};
        log('设置控制线', inspect(flags, options));
        prepareTimer = setTimeout(() => { error('SET TIMEOUT', new Error('设置控制线超过 5 秒')); finish(1); }, 5000);
        port.set(flags, err => {
            clearTimeout(prepareTimer);
            if (stopping) return;
            if (err) { error('SET RTS/DTR', err); finish(1); return; }
            log('SET OK', '驱动接受控制线设置；不等于已验证物理电平');
            start();
        });
    });
}

if (require.main === module) main().catch(err => { error('MAIN', err); process.exitCode = 1; });
module.exports = { parseResponse, crc16, parseArgs, PROBES, parseRegistryPorts, comparePorts, listRegistryPorts, isUnsupportedWindowsDrain };

