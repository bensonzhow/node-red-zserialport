'use strict';
// Maintainer-only build. Installation never executes a compiler or downloads binaries.
const { spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const root = path.resolve(__dirname, '..');
const zig = process.env.ZIG || 'zig';
const version = spawnSync(zig, ['version'], { encoding: 'utf8' });
if (version.status !== 0 || version.stdout.trim() !== '0.13.0') throw new Error('Build requires Zig 0.13.0; set ZIG to its executable');
const zigEnv = JSON.parse(spawnSync(zig, ['env'], { encoding: 'utf8' }).stdout);
const manifest = { compiler: 'Zig 0.13.0', targetOS: 'Windows 7 (6.1)', protocol: 1, binaries: {} };
for (const [arch, target] of [['x86', 'x86-windows.win7-gnu'], ['x64', 'x86_64-windows.win7-gnu']]) {
    const relative = 'bin/windows/' + arch + '/zserial-sync.exe';
    fs.mkdirSync(path.dirname(path.join(root, relative)), { recursive: true });
    const result = spawnSync(zig, ['cc', '-target', target, '-isystem', path.join(zigEnv.lib_dir, 'libc/include/any-windows-any'), '-O2', '-Wall', '-Wextra', '-Werror', '-nostdlib', '-fno-builtin', '-s', '-Wl,--entry=mainCRTStartup', '-lkernel32', '-luser32',
        'native/win32-sync/bridge.c', '-o', relative], { cwd: root, stdio: 'inherit' });
    if (result.status !== 0) throw new Error('Build failed: ' + arch);
    manifest.binaries[arch] = { file: relative, sha256: crypto.createHash('sha256').update(fs.readFileSync(path.join(root, relative))).digest('hex') };
}
fs.writeFileSync(path.join(root, 'bin/windows/manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
require('./verify-win32-sync');
