'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const root = path.resolve(__dirname, '..');
const manifest = require('../bin/windows/manifest.json');
for (const [arch, machine] of [['x86', 0x14c], ['x64', 0x8664]]) {
    const entry = manifest.binaries[arch];
    const b = fs.readFileSync(path.join(root, entry.file));
    if (crypto.createHash('sha256').update(b).digest('hex') !== entry.sha256) throw new Error(arch + ': checksum mismatch');
    const pe = b.readUInt32LE(0x3c), opt = pe + 24;
    if (b.toString('ascii', pe, pe + 4) !== 'PE\0\0' || b.readUInt16LE(pe + 4) !== machine) throw new Error(arch + ': wrong PE architecture');
    const major = b.readUInt16LE(opt + 48), minor = b.readUInt16LE(opt + 50);
    if (major > 6 || (major === 6 && minor > 1)) throw new Error(arch + ': subsystem newer than Win7');
    const sections = pe + 24 + b.readUInt16LE(pe + 20);
    const offset = rva => {
        for (let i = 0; i < b.readUInt16LE(pe + 6); i++) {
            const s = sections + i * 40, start = b.readUInt32LE(s + 12);
            if (rva >= start && rva < start + Math.max(b.readUInt32LE(s + 8), b.readUInt32LE(s + 16))) return b.readUInt32LE(s + 20) + rva - start;
        }
        throw new Error('Invalid PE RVA');
    };
    const directory = opt + (arch === 'x64' ? 112 : 96);
    let imp = offset(b.readUInt32LE(directory + 8));
    const dlls = [];
    while (b.readUInt32LE(imp + 12)) {
        const name = offset(b.readUInt32LE(imp + 12));
        const dll = b.toString('ascii', name, b.indexOf(0, name)).toLowerCase();
        if (!['kernel32.dll', 'user32.dll'].includes(dll)) throw new Error('Unexpected runtime dependency: ' + dll);
        dlls.push(dll); imp += 20;
    }
    console.log(arch + ': SHA256/PE OK, subsystem ' + major + '.' + minor + ', imports ' + dlls.join(', '));
}
