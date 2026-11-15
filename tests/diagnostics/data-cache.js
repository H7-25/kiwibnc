// Synthetic measurements only; never opens a database or connects to a service.
// Run with the same Node as the worker: node tests/diagnostics/data-cache.js
const { execFileSync } = require('child_process');
const LRU = require('lru-cache');

const cases = { ascii: 'A'.repeat(32), latin1: 'é'.repeat(32),
    unicode: '漢'.repeat(32), emoji: '🙂'.repeat(16) };
const mode = process.argv[2];
if (!mode) {
    for (const name of Object.keys(cases)) {
        process.stdout.write(execFileSync(process.execPath, ['--expose-gc', __filename, name], { encoding: 'utf8' }));
    }
} else {
    if (!global.gc || !Object.hasOwn(cases, mode)) throw new Error('Use the script without arguments');
    const cache = new LRU({ max: 50000000, length: (value, key) => key.length });
    global.gc();
    const before = process.memoryUsage().heapUsed;
    for (let n = 0; n < 100000; n++) {
        // Flatten each key; do not retain a separate input array in the measurement.
        const key = Buffer.from(`${String(n).padStart(8, '0')}:${cases[mode]}`, 'utf8').toString('utf8');
        cache.set(key, n + 1);
    }
    global.gc();
    const heapDelta = process.memoryUsage().heapUsed - before;
    console.log(JSON.stringify({ node: process.version, lru: require('lru-cache/package.json').version,
        case: mode, entries: cache.itemCount, keyCodeUnits: cache.length,
        utf16PayloadBytes: cache.length * 2, heapDeltaBytes: heapDelta }));
}
