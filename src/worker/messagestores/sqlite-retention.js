// All reads are bounded seeks in the existing (user_id, bufferref, time, rowid)
// index. rowid is SQLite's implicit final key in this ordinary table's index.
const { performance } = require('perf_hooks');
const INDEX = 'logs INDEXED BY logs_user_id_ts';
const SQL = {
    firstUser: `SELECT user_id FROM ${INDEX} WHERE user_id IS NOT NULL ORDER BY user_id LIMIT 1`,
    nextUser: `SELECT user_id FROM ${INDEX} WHERE user_id > ? ORDER BY user_id LIMIT 1`,
    firstBuffer: `SELECT bufferref FROM ${INDEX} WHERE user_id = ? AND bufferref IS NOT NULL ORDER BY bufferref LIMIT 1`,
    nextBuffer: `SELECT bufferref FROM ${INDEX} WHERE user_id = ? AND bufferref > ? ORDER BY bufferref LIMIT 1`,
    buffer: 'SELECT CAST(data AS TEXT) AS name FROM data WHERE id = ?',
    emptyPrefix: "SELECT id FROM data WHERE data = ''",
    firstPage: `SELECT rowid, * FROM ${INDEX} WHERE user_id = ? AND bufferref = ? AND time < ? ORDER BY time, rowid LIMIT ?`,
    sameTime: `SELECT rowid, * FROM ${INDEX} WHERE user_id = ? AND bufferref = ? AND time = ? AND rowid > ? ORDER BY rowid LIMIT ?`,
    laterTime: `SELECT rowid, * FROM ${INDEX} WHERE user_id = ? AND bufferref = ? AND time > ? AND time < ? ORDER BY time, rowid LIMIT ?`,
    deleteLog: 'DELETE FROM logs WHERE rowid = ?',
    deleteTarget: `DELETE FROM chathistory_targets
        WHERE user_id = ? AND network_id = ? AND bufferref = ? AND last_time = -1`,
};

const immediate = () => new Promise(resolve => setImmediate(resolve));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

class SqliteRetention {
    constructor(store, options = {}) {
        this.store = store;
        this.db = store.db;
        this.batchSize = options.batchSize === undefined ? 150 : options.batchSize;
        if (!Number.isInteger(this.batchSize) || this.batchSize < 1 || this.batchSize > 150) {
            throw new RangeError('Retention batchSize must be between 1 and 150');
        }
        this.pause = options.pause || sleep;
        this.shouldContinue = options.shouldContinue || (() => true);
        this.statements = Object.fromEntries(Object.entries(SQL).map(([key, sql]) => [key, this.db.prepare(sql)]));
        this.days = {
            channels: store.retentionDaysChannels,
            pms: store.retentionDaysPMs,
            server: store.retentionDaysServer,
        };
        this.cutoffs = {};
        const now = options.now === undefined ? Date.now() : options.now;
        for (const [kind, days] of Object.entries(this.days)) {
            if (days > 0) {
                const date = new Date(now);
                date.setDate(date.getDate() - days);
                this.cutoffs[kind] = date.getTime();
            }
        }
        this.totals = { channels: 0, pms: 0, server: 0 };
    }

    async retry(fn) {
        let busyRetries = 0;
        let transactionWaits = 0;
        while (true) {
            if (this.db.inTransaction) {
                if (transactionWaits++ >= 50) throw new Error('Database busy with other transactions');
                await this.pause(100);
                continue;
            }
            try {
                return fn();
            } catch (err) {
                // better-sqlite3 exposes extended result codes too. A WAL
                // snapshot invalidated by another writer needs a fresh transaction.
                const busy = typeof err.code === 'string' &&
                    (err.code === 'SQLITE_BUSY' || err.code.startsWith('SQLITE_BUSY_'));
                if (!busy || busyRetries++ >= 10) throw err;
                l.warn('SQLite busy during retention, retrying');
                await this.pause(200);
            }
        }
    }

    // Called synchronously inside the same transaction as deletion. Re-resolving
    // data IDs per page also tolerates data rowid reuse between asynchronous yields.
    processPage(userId, bufferId, cursor) {
        const s = this.statements;
        const buffer = s.buffer.get(bufferId);
        const kind = buffer && typeof buffer.name === 'string'
            ? (/^[#&]/.test(buffer.name) ? 'channels' : 'pms') : null;
        const horizons = [this.cutoffs.server, kind && this.cutoffs[kind]].filter(Number.isFinite);
        if (!horizons.length) return { last: null, counts: {} };
        const horizon = Math.max(...horizons);
        let rows;
        if (!cursor) {
            rows = s.firstPage.all(userId, bufferId, horizon, this.batchSize);
        } else {
            // Two seeks avoid rescanning earlier rowids when a timestamp has a
            // very large number of messages. Do not replace with OFFSET or an OR.
            rows = s.sameTime.all(userId, bufferId, cursor.time, cursor.rowid, this.batchSize);
            if (rows.length < this.batchSize) {
                rows.push(...s.laterTime.all(userId, bufferId, cursor.time, horizon, this.batchSize - rows.length));
            }
        }

        const emptyPrefix = s.emptyPrefix.get();
        const deleted = [];
        const targets = new Map();
        const counts = { channels: 0, pms: 0, server: 0 };
        for (const row of rows) {
            const category = emptyPrefix && row.prefixref === emptyPrefix.id ? 'server' : kind;
            if (!category || !(row.time < this.cutoffs[category])) continue;
            s.deleteLog.run(row.rowid);
            deleted.push(row);
            counts[category]++;
            const key = JSON.stringify([row.user_id, row.network_id, row.bufferref]);
            const previous = targets.get(key);
            if (!previous || previous.deleted_time < row.time) {
                targets.set(key, {
                    user_id: row.user_id, network_id: row.network_id,
                    bufferref: row.bufferref, deleted_time: row.time,
                });
            }
        }
        const targetsStarted = performance.now();
        for (const target of targets.values()) {
            this.store.stmtRefreshChathistoryTarget.run(target);
            s.deleteTarget.run(target.user_id, target.network_id, target.bufferref);
        }
        const targetsMs = performance.now() - targetsStarted;
        // At most 150 * 5 references. If cleanup fails, rollback also restores
        // the logs and targets: no pending IDs can be lost, even across restart.
        const cleanupStarted = performance.now();
        this.store.runDataCleanup(deleted);
        const cleanupMs = performance.now() - cleanupStarted;
        const last = rows[rows.length - 1];
        return { last: last ? { time: last.time, rowid: last.rowid } : null, counts, targetsMs, cleanupMs };
    }

    async run() {
        if (!Object.keys(this.cutoffs).length) return this.totals;
        const s = this.statements;
        const transactPage = this.db.transaction((...args) => this.processPage(...args));
        let user = await this.retry(() => s.firstUser.get());
        while (user && this.shouldContinue()) {
            await immediate();
            let buffer = await this.retry(() => s.firstBuffer.get(user.user_id));
            while (buffer && this.shouldContinue()) {
                await immediate();
                let cursor = null;
                while (this.shouldContinue()) {
                    const page = await this.retry(() => {
                        const started = performance.now();
                        const result = transactPage(user.user_id, buffer.bufferref, cursor);
                        // Include COMMIT, but not asynchronous backoff between attempts.
                        result.durationMs = performance.now() - started;
                        return result;
                    });
                    if (page.durationMs > 100) {
                        l.warn(`Slow retention batch: ${page.durationMs.toFixed(1)}ms ` +
                            `(TARGETS ${(page.targetsMs || 0).toFixed(1)}ms, ` +
                            `orphan cleanup ${(page.cleanupMs || 0).toFixed(1)}ms)`);
                    }
                    // Only advance after commit, including over protected rows.
                    cursor = page.last;
                    for (const [kind, count] of Object.entries(page.counts)) {
                        this.totals[kind] += count;
                        if (count) {
                            const label = kind === 'pms' ? 'PMs' : kind === 'server' ? 'server tab' : kind;
                            l.info(`Retention cleanup (${label}, >${this.days[kind]} days) removed ${count} messages`);
                        }
                    }
                    if (!cursor) break;
                    // Yield even if every examined row was protected.
                    await this.pause(50);
                }
                buffer = await this.retry(() => s.nextBuffer.get(user.user_id, buffer.bufferref));
            }
            user = await this.retry(() => s.nextUser.get(user.user_id));
        }
        return this.totals;
    }
}

module.exports = SqliteRetention;
module.exports.SQL = SQL;
