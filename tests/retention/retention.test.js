const assert = require('node:assert/strict');
const { test } = require('node:test');
const { Store, backend, substitutedDependencies } = require('./database');
const { SQL } = require('../../src/worker/messagestores/sqlite-retention');
const { spawnSync } = require('node:child_process');

const DAY = 86400000;
const NOW = Date.UTC(2026, 10, 15, 12);
const fastPause = () => new Promise(resolve => setImmediate(resolve));

async function fixture(t, days = { channels: 60, pms: 30, server: 60 }) {
    // Disable scheduled work; exercise the actual store and scanner explicitly.
    const store = new Store({
        relativePath: p => p,
        get: () => ({ database: ':memory:', vacuum: false }),
    });
    await store.init();
    t.after(() => store.db.close());
    store.retentionDaysChannels = days.channels;
    store.retentionDaysPMs = days.pms;
    store.retentionDaysServer = days.server;
    const add = (name, age, options = {}) => {
        const user = options.user === undefined ? 1 : options.user;
        const network = options.network === undefined ? 1 : options.network;
        const buffer = store.dataId(name);
        const time = options.time === undefined ? NOW - age * DAY : options.time;
        const refs = [store.dataId('{}'), store.dataId(options.text || 'shared text'),
            store.dataId(options.server ? '' : 'nick'), store.dataId(name)];
        const msgid = options.msgid || `${user}:${network}:${Math.random()}`;
        const result = store.db.prepare(`INSERT INTO logs
            (user_id, network_id, bufferref, time, type, msgid, msgtagsref, dataref, prefixref, paramsref)
            VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`).run(user, network, buffer, time, msgid, ...refs);
        if (options.target !== false && user !== null) {
            store.stmtUpdateChathistoryTarget.run(user, network, buffer, time);
        }
        return { id: Number(result.lastInsertRowid), buffer, refs, msgid, time };
    };
    const ids = () => store.db.prepare('SELECT msgid FROM logs ORDER BY msgid').all().map(r => r.msgid);
    const run = options => store.runRetentionCleanup({ now: NOW, batchSize: 3,
        pause: fastPause, throwOnError: true, ...options });
    return { store, add, ids, run };
}

test(`retention uses real SQLite: ${backend}`, async t => {
    const { store } = await fixture(t);
    t.diagnostic(`SQLite ${store.db.prepare('SELECT sqlite_version() AS version').get().version}`);
    t.diagnostic(`Store dependency substitutes: ${substitutedDependencies}`);
    assert.equal(store.db.pragma('temp_store', { simple: true }), 2);
});

test('module can load before bootstrap, then resolves the real global logger at execution', async t => {
    const loaded = spawnSync(process.execPath, ['-e',
        'delete global.l; require(process.argv[1]);',
        require.resolve('../../src/worker/messagestores/sqlite-retention')], { encoding: 'utf8' });
    assert.equal(loaded.status, 0, loaded.stderr);
    const { store, add, run } = await fixture(t);
    const previousLogger = global.l;
    const logger = require('../../src/libs/logger')('retention-test');
    logger.level = logger.levels.error;
    const calls = { info: 0, warn: 0 };
    for (const method of Object.keys(calls)) {
        const original = logger[method];
        logger[method] = (...args) => { calls[method]++; return original(...args); };
    }
    global.l = logger;
    try {
        add('pm', 100);
        const cleanup = store.runDataCleanup;
        let fail = true;
        store.runDataCleanup = function(rows) {
            cleanup.call(this, rows);
            if (fail) {
                fail = false;
                throw Object.assign(new Error('busy'), { code: 'SQLITE_BUSY' });
            }
        };
        await run();
        assert.ok(calls.info > 0);
        assert.equal(calls.warn, 1);
    } finally {
        global.l = previousLogger;
    }
});

test('independent channel, PM and server policies, including server rows in channels', async t => {
    const { add, ids, run } = await fixture(t);
    add('#c', 61, { msgid: 'channel-old' });
    add('#c', 40, { msgid: 'channel-kept' });
    add('pm', 31, { msgid: 'pm-old' });
    add('pm', 29, { msgid: 'pm-kept' });
    add('pm', 40, { server: true, msgid: 'server-kept' });
    add('pm', 61, { server: true, msgid: 'server-old' });
    add('#c', 40, { server: true, msgid: 'channel-server-kept' });
    const totals = await run();
    assert.equal(totals.channels, 1);
    assert.equal(totals.pms, 1);
    assert.equal(totals.server, 1);
    assert.deepEqual(ids(), ['channel-kept', 'channel-server-kept', 'pm-kept', 'server-kept']);
});

test('identical timestamps span many pages, accounts and networks; TARGETS may be incomplete', async t => {
    const { store, add, ids, run } = await fixture(t);
    for (const user of [-2, 0, 1, 2]) {
        for (const network of [1, 2]) {
            for (let n = 0; n < 11; n++) add('same-buffer', 70, { user, network, target: n !== 0 });
            add('same-buffer', 1, { user, network, msgid: `kept:${user}:${network}` });
        }
    }
    const totals = await run();
    assert.equal(totals.pms, 88);
    assert.equal(ids().length, 8);
    const targets = store.db.prepare('SELECT * FROM chathistory_targets').all();
    assert.equal(targets.length, 8);
    assert.ok(targets.every(row => row.last_time === NOW - DAY));
    add('old-without-target', 70, { target: false });
    assert.equal((await run()).pms, 1);
});

test('many protected rows before deletable rows advance the examined cursor', async t => {
    const { add, ids, run } = await fixture(t, { channels: 60, pms: 30, server: 0 });
    // Same timestamp: filtering before LIMIT could repeatedly examine the prefix.
    for (let n = 0; n < 100; n++) add('mixed', 40, { server: true, msgid: `protected:${n}` });
    add('mixed', 40, { msgid: 'expired' });
    let pauses = 0;
    const counts = await run({ pause: async () => {
        assert.ok(++pauses < 50, 'cursor must not loop over protected rows');
        await fastPause();
    } });
    assert.equal(counts.pms, 1);
    assert.equal(ids().length, 100);
});

test('disabled categories remain protected; server-only retention runs', async t => {
    const { add, ids, run } = await fixture(t, { channels: 0, pms: 0, server: 60 });
    add('#c', 100, { msgid: 'channel' });
    add('pm', 100, { msgid: 'pm' });
    add('pm', 61, { server: true, msgid: 'server' });
    assert.equal((await run()).server, 1);
    assert.deepEqual(ids(), ['channel', 'pm']);
});

test('a shorter server policy cannot delete protected conversations', async t => {
    const { store, add, ids, run } = await fixture(t, { channels: 60, pms: 60, server: 30 });
    const kept = add('mixed', 40, { msgid: 'conversation' });
    add('mixed', 31, { server: true, msgid: 'server' });
    assert.equal((await run()).server, 1);
    assert.deepEqual(ids(), ['conversation']);
    assert.equal(store.db.prepare('SELECT last_time FROM chathistory_targets').get().last_time, kept.time);
});

test('the production pause lets timers run even when pages contain only protected rows', async t => {
    const { add, run } = await fixture(t, { channels: 0, pms: 30, server: 0 });
    for (let n = 0; n < 8; n++) add('mixed', 40, { server: true });
    let ticks = 0;
    const timer = setInterval(() => ticks++, 5);
    try {
        await run({ pause: undefined });
        assert.ok(ticks > 0);
    } finally {
        clearInterval(timer);
    }
});

test('empty history and all categories disabled are no-ops', async t => {
    const { store, add, ids, run } = await fixture(t, { channels: 0, pms: 0, server: 0 });
    assert.equal(Object.values(await run()).reduce((a, b) => a + b, 0), 0);
    add('pm', 100);
    await run();
    assert.equal(ids().length, 1);
    assert.equal(store.cleanupRunning, false);
});

test('strict cutoff preserves the boundary, future rows and unclassifiable references', async t => {
    const { store, add, ids, run } = await fixture(t);
    const cutoff = new Date(NOW);
    cutoff.setDate(cutoff.getDate() - 30);
    add('pm', 0, { time: cutoff.getTime(), msgid: 'boundary' });
    add('pm', 0, { time: cutoff.getTime() - 1, msgid: 'expired' });
    add('pm', -2, { msgid: 'future' });
    const bad = add('broken', 100, { msgid: 'broken', target: false });
    store.db.prepare('UPDATE logs SET bufferref = NULL WHERE rowid = ?').run(bad.id);
    const nullName = add('null-name', 100, { msgid: 'null-name', target: false });
    store.db.prepare('UPDATE data SET data = NULL WHERE id = ?').run(nullName.buffer);
    await run();
    assert.deepEqual(ids(), ['boundary', 'broken', 'future', 'null-name']);
});

test('orphan cleanup preserves all five kinds of live references and invalidates cache', async t => {
    const { store, add, run } = await fixture(t);
    const expired = add('old', 100, { text: 'unique-old-text' });
    add('live', 1);
    // Keep each old value alive through a different reference column.
    for (const [index, column] of ['bufferref', 'msgtagsref', 'dataref', 'prefixref', 'paramsref'].entries()) {
        const reference = store.dataId(`reference-${index}`);
        const live = add(`live-${index}`, 1);
        store.db.prepare(`UPDATE logs SET ${column} = ? WHERE rowid = ?`).run(reference, live.id);
        const old = add(`expired-${index}`, 100);
        store.db.prepare(`UPDATE logs SET dataref = ? WHERE rowid = ?`).run(reference, old.id);
    }
    await run();
    assert.equal(store.db.prepare('SELECT id FROM data WHERE id = ?').get(expired.refs[1]), undefined);
    assert.equal(store.dataCache.get('unique-old-text'), undefined);
    for (let n = 0; n < 5; n++) {
        assert.ok(store.db.prepare('SELECT id FROM data WHERE data = ?').get(`reference-${n}`));
    }
    const orphan = store.db.prepare(`SELECT d.id FROM data d WHERE NOT EXISTS (
        SELECT 1 FROM logs WHERE bufferref=d.id OR msgtagsref=d.id OR dataref=d.id OR prefixref=d.id OR paramsref=d.id
    ) AND d.data='unique-old-text'`).get();
    assert.equal(orphan, undefined);
});

test('cleanup failure after SQL work rolls back logs, data and TARGETS and remains recoverable', async t => {
    const { store, add, ids, run } = await fixture(t);
    const old = add('pm', 100, { msgid: 'old', text: 'unique' });
    const original = store.runDataCleanup;
    store.runDataCleanup = function(rows) {
        original.call(this, rows);
        throw new Error('injected orphan failure');
    };
    await assert.rejects(run(), /injected orphan failure/);
    assert.deepEqual(ids(), ['old']);
    assert.ok(store.db.prepare('SELECT id FROM data WHERE id = ?').get(old.refs[1]));
    assert.equal(store.db.prepare('SELECT last_time FROM chathistory_targets').get().last_time, old.time);
    assert.equal(store.cleanupRunning, false);
    store.runDataCleanup = original;
    assert.equal((await run()).pms, 1);
    assert.equal(store.db.prepare('SELECT count(*) AS n FROM chathistory_targets').get().n, 0);
});

test('nested cleanup rolls back with the outer transaction; cache misses safely recover IDs', async t => {
    const { store } = await fixture(t);
    const id = store.dataId('orphan');
    assert.equal(store.dataCache.get('orphan'), id);
    const abort = store.db.transaction(() => {
        assert.equal(store.db.inTransaction, true);
        store.cleanupOrphanedData([id]);
        assert.equal(store.db.inTransaction, true);
        assert.equal(store.db.prepare('SELECT id FROM data WHERE id = ?').get(id), undefined);
        throw new Error('outer rollback');
    });
    assert.throws(abort, /outer rollback/);
    assert.equal(store.db.inTransaction, false);
    assert.ok(store.db.prepare('SELECT id FROM data WHERE id = ?').get(id));
    // Cache invalidation is conservative: a rolled-back deletion causes a miss,
    // never a cached ID for a missing row. dataId must rediscover the original ID.
    assert.equal(store.dataCache.get('orphan'), undefined);
    assert.equal(store.dataId('orphan'), id);
    store.db.transaction(() => store.cleanupOrphanedData([id]))();
    assert.equal(store.db.prepare('SELECT id FROM data WHERE id = ?').get(id), undefined);
    assert.equal(store.dataCache.get('orphan'), undefined);
});

test('SQLITE_BUSY rollback retries the same page before advancing', async t => {
    const { store, add, ids, run } = await fixture(t);
    for (let n = 0; n < 8; n++) add('pm', 100);
    const original = store.runDataCleanup;
    let attempts = 0;
    store.runDataCleanup = function(rows) {
        original.call(this, rows);
        if (++attempts === 1) throw Object.assign(new Error('locked'), { code: 'SQLITE_BUSY' });
    };
    let retries = 0;
    const result = await run({ pause: async ms => {
        if (ms === 200) { retries++; assert.equal(ids().length, 8); }
        await fastPause();
    } });
    assert.equal(retries, 1);
    assert.equal(result.pms, 8);
    assert.equal(ids().length, 0);
});

test('exhausted retries leave messages and allow a later cycle', async t => {
    const { store, add, ids, run } = await fixture(t);
    add('pm', 100);
    const original = store.runDataCleanup;
    let attempts = 0;
    store.runDataCleanup = () => {
        attempts++;
        throw Object.assign(new Error('locked'), { code: 'SQLITE_BUSY' });
    };
    await assert.rejects(run(), /locked/);
    assert.equal(attempts, 11);
    assert.equal(ids().length, 1);
    store.runDataCleanup = original;
    assert.equal((await run()).pms, 1);
});

test('extended SQLITE_BUSY_SNAPSHOT retries a new transaction without skipping the page', async t => {
    const { store, add, ids, run } = await fixture(t);
    add('pm', 100);
    const cleanup = store.runDataCleanup;
    let failed = false;
    store.runDataCleanup = function(rows) {
        if (!failed) {
            failed = true;
            throw Object.assign(new Error('snapshot changed'), { code: 'SQLITE_BUSY_SNAPSHOT' });
        }
        return cleanup.call(this, rows);
    };
    let retries = 0;
    const result = await run({ pause: async ms => {
        if (ms === 200) {
            retries++;
            assert.equal(ids().length, 1);
            assert.equal(store.db.inTransaction, false);
        }
        await fastPause();
    } });
    assert.equal(retries, 1);
    assert.equal(result.pms, 1);
    assert.equal(ids().length, 0);
});

test('TARGETS update failure rolls back deletion before the cursor can advance', async t => {
    const { store, add, ids, run } = await fixture(t);
    add('pm', 100, { msgid: 'old' });
    const statement = store.stmtRefreshChathistoryTarget;
    store.stmtRefreshChathistoryTarget = { run() { throw new Error('target failure'); } };
    await assert.rejects(run(), /target failure/);
    assert.deepEqual(ids(), ['old']);
    store.stmtRefreshChathistoryTarget = statement;
    assert.equal((await run()).pms, 1);
    assert.equal(store.db.prepare('SELECT count(*) AS n FROM chathistory_targets').get().n, 0);
});

test('default pages bound orphan work to at most 750 candidate references', async t => {
    const { store, add, run } = await fixture(t);
    for (let n = 0; n < 310; n++) add('pm', 100, { text: `unique-${n}` });
    const cleanup = store.cleanupOrphanedData;
    let chunks = 0;
    store.cleanupOrphanedData = function(ids) {
        assert.ok(ids.length <= 750);
        chunks++;
        return cleanup.call(this, ids);
    };
    assert.equal((await run({ batchSize: 150 })).pms, 310);
    assert.equal(chunks, 3);
});

test('interruption and concurrent inserts do not lose work or protected messages', async t => {
    const { add, ids, run } = await fixture(t);
    for (let n = 0; n < 10; n++) add('pm', 100);
    let active = true;
    await run({ shouldContinue: () => active, pause: async () => {
        add('pm', 1, { msgid: 'new-protected' });
        add('pm', 101, { msgid: 'backdated' });
        active = false;
    } });
    assert.equal(ids().length, 9);
    await run();
    assert.deepEqual(ids(), ['new-protected']);
});

test('CHATHISTORY queries retain surviving messages and TARGETS after retention', async t => {
    const { store, add, run } = await fixture(t);
    add('pm', 100, { msgid: 'expired' });
    const first = add('pm', 2, { msgid: 'first' });
    const second = add('pm', 1, { msgid: 'second' });
    await run();
    const texts = rows => rows.map(row => row.tags.time);
    assert.equal((await store.getMessagesBeforeTime(1, 1, 'pm', NOW, 50)).length, 2);
    assert.equal((await store.getMessagesFromTime(1, 1, 'pm', first.time, 50)).length, 1);
    assert.equal((await store.getMessagesBeforeMsgId(1, 1, 'pm', 'second', 50)).length, 2);
    assert.equal((await store.getMessagesFromMsgId(1, 1, 'pm', 'first', 50)).length, 1);
    assert.equal((await store.getMessagesBetween(1, 1, 'pm', { type: 'timestamp', value: first.time },
        { type: 'timestamp', value: NOW }, 50)).length, 2);
    const latest = await store.getMessagesBeforeTime(1, 1, 'pm', NOW, 1);
    assert.equal(texts(latest)[0], new Date(second.time).toISOString());
    const targets = await store.getTargets(1, 1, 0, NOW, 50);
    assert.equal(targets.length, 1);
    assert.equal(targets[0].time, second.time);
});

test('all enumeration and page plans seek the existing index with no temporary sort', async t => {
    const { store, add } = await fixture(t);
    for (let n = 0; n < 500; n++) add('pm', 100);
    const params = {
        firstUser: [], nextUser: [0], firstBuffer: [1], nextBuffer: [1, 0],
        firstPage: [1, 1, NOW, 3], sameTime: [1, 1, NOW - 100 * DAY, 250, 3],
        laterTime: [1, 1, NOW - 101 * DAY, NOW, 3],
    };
    for (const [name, bindings] of Object.entries(params)) {
        const plan = store.db.prepare(`EXPLAIN QUERY PLAN ${SQL[name]}`).all(...bindings).map(r => r.detail).join('\n');
        assert.match(plan, /SEARCH logs USING (?:COVERING )?INDEX logs_user_id_ts/);
        assert.doesNotMatch(plan, /SCAN logs|TEMP B-TREE/);
        if (name === 'sameTime') assert.match(plan, /rowid>\?/);
    }
});

test('orphan checks use all five reference indexes instead of scanning logs', async t => {
    const { store } = await fixture(t);
    const id = store.dataId('orphan');
    const prepare = store.db.prepare.bind(store.db);
    let cleanupSql;
    store.db.prepare = sql => {
        if (/DELETE FROM data/.test(sql)) cleanupSql = sql;
        return prepare(sql);
    };
    store.cleanupOrphanedData([id]);
    assert.ok(cleanupSql);
    const plan = prepare(`EXPLAIN QUERY PLAN ${cleanupSql}`).all(id).map(row => row.detail).join('\n');
    for (const column of ['bufferref', 'msgtagsref', 'dataref', 'prefixref', 'paramsref']) {
        assert.match(plan, new RegExp(`SEARCH logs USING COVERING INDEX logs_${column}`));
    }
    assert.doesNotMatch(plan, /SCAN logs/);
});

test('retention creates no TEMP objects even with a large unrelated data dictionary', async t => {
    const { store, add, run } = await fixture(t);
    store.db.transaction(() => {
        for (let n = 0; n < 3000; n++) store.dataId(`not-a-buffer-${n}`);
    })();
    add('pm', 100);
    await run();
    assert.equal(store.db.prepare('SELECT count(*) AS n FROM sqlite_temp_master').get().n, 0);
    for (const sql of Object.values(SQL)) assert.doesNotMatch(sql, /CREATE|DISTINCT|OFFSET/i);
});

test('deterministic mixed history matches an independent retention and TARGETS oracle', async t => {
    for (const days of [
        { channels: 60, pms: 30, server: 60 },
        { channels: 0, pms: 60, server: 30 },
        { channels: 30, pms: 0, server: 0 },
    ]) {
        await t.test(JSON.stringify(days), async child => {
            const { store, add, ids, run } = await fixture(child, days);
            let seed = 123;
            const random = n => {
                seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
                return seed % n;
            };
            const expected = [];
            for (let n = 0; n < 1200; n++) {
                const name = `${random(2) ? '#' : ''}buffer-${random(7)}`;
                const server = random(5) === 0;
                const category = server ? 'server' : name.startsWith('#') ? 'channels' : 'pms';
                const time = NOW - random(100) * DAY;
                const row = add(name, 0, { time, server, user: random(4), network: random(3), msgid: `row-${n}` });
                const cutoff = new Date(NOW);
                cutoff.setDate(cutoff.getDate() - days[category]);
                if (days[category] <= 0 || time >= cutoff.getTime()) {
                    expected.push(row);
                }
            }
            await run({ batchSize: 150 });
            assert.deepEqual(ids(), expected.map(row => row.msgid).sort());
            const oracle = store.db.prepare(`SELECT user_id, network_id, bufferref, MAX(time) AS last_time
                FROM logs GROUP BY user_id, network_id, bufferref
                ORDER BY user_id, network_id, bufferref`).all().map(row => ({ ...row }));
            const targets = store.db.prepare(`SELECT * FROM chathistory_targets
                ORDER BY user_id, network_id, bufferref`).all().map(row => ({ ...row }));
            assert.deepEqual(targets, oracle);
            assert.equal(store.db.prepare(`SELECT count(*) AS n FROM data d WHERE NOT EXISTS (
                SELECT 1 FROM logs WHERE bufferref=d.id OR msgtagsref=d.id OR dataref=d.id OR prefixref=d.id OR paramsref=d.id
            )`).get().n, 0);
        });
    }
});
