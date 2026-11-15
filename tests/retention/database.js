// The installed project dependencies are preferred. A bundled modern Node can
// also run these retention tests without installing packages: only unrelated
// IRC, cache and StatsD dependencies are substituted; SQLite remains real.
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { createRequire } = require('node:module');
const requireReal = process.env.KIWIBNC_RETENTION_REQUIRE_REAL === '1';

function database() {
    try {
        const Database = require('better-sqlite3');
        return { Database, backend: 'better-sqlite3' };
    } catch (err) {
        if (err.code !== 'MODULE_NOT_FOUND') throw err;
        if (requireReal) throw new Error('Real retention tests require installed better-sqlite3', { cause: err });
        const { DatabaseSync } = require('node:sqlite');
        class Database extends DatabaseSync {
            pragma(sql, options) {
                const rows = this.prepare(`PRAGMA ${sql}`).all();
                return options && options.simple ? Object.values(rows[0])[0] : rows;
            }
            transaction(fn) {
                return (...args) => {
                    const nested = !!this.inTransaction;
                    this.exec(nested ? 'SAVEPOINT retention_test' : 'BEGIN');
                    if (!nested) this.inTransaction = true;
                    try {
                        const result = fn(...args);
                        this.exec(nested ? 'RELEASE retention_test' : 'COMMIT');
                        return result;
                    } catch (error) {
                        this.exec(nested ? 'ROLLBACK TO retention_test; RELEASE retention_test' : 'ROLLBACK');
                        throw error;
                    } finally {
                        if (!nested) this.inTransaction = false;
                    }
                };
            }
        }
        return { Database, backend: 'node:sqlite (compatibility adapter)' };
    }
}

const { Database, backend } = database();
const logger = { info() {}, debug() {}, warn() {}, error() {} };
global.l = logger;
let Store;
let substitutedDependencies = false;
try {
    Store = require('../../src/worker/messagestores/sqlite');
} catch (err) {
    if (err.code !== 'MODULE_NOT_FOUND') throw err;
    if (requireReal) throw new Error('Real retention tests require all store dependencies; no substitutes allowed', { cause: err });
    substitutedDependencies = true;
    const filename = path.resolve(__dirname, '../../src/worker/messagestores/sqlite.js');
    const localRequire = createRequire(filename);
    const stats = { gauge() {}, increment() {}, timerStart: () => ({ stop() {} }) };
    stats.makePrefix = () => stats;
    class Cache extends Map {
        constructor() { super(); }
        del(key) { return this.delete(key); }
    }
    class Message { constructor() { this.tags = {}; this.params = []; } }
    const substitutes = {
        'better-sqlite3': Database,
        'lru-cache': Cache,
        '../../libs/stats': { instance: () => stats },
        '../../libs/helpers': { isoTime: date => (date || new Date()).toISOString() },
        'irc-framework': { Message },
    };
    const mod = { exports: {} };
    const scope = {
        module: mod, exports: mod.exports, __dirname: path.dirname(filename),
        require: name => Object.prototype.hasOwnProperty.call(substitutes, name) ? substitutes[name] : localRequire(name),
        l: logger, Date, setTimeout, setInterval, setImmediate, clearTimeout,
    };
    vm.runInNewContext(fs.readFileSync(filename, 'utf8'), scope, { filename });
    Store = mod.exports;
}

module.exports = { Store, backend, substitutedDependencies };
