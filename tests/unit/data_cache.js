const SqliteMessageStore = require('../../src/worker/messagestores/sqlite');

describe('SQLite dataCache on-demand diagnostics (real driver and LRU)', () => {
    let store;
    beforeEach(async () => {
        global.l = { debug: jest.fn(), info: jest.fn(), warn: jest.fn(), error: jest.fn() };
        store = new SqliteMessageStore({
            relativePath: value => value,
            get: () => ({ database: ':memory:', vacuum: false }),
        });
        await store.init();
    });
    afterEach(() => { store.db.close(); delete global.l; });

    test('keeps the existing eviction budget and counts hits without extra SQL', () => {
        const reads = jest.spyOn(store.stmtGetExistingDataId, 'get');
        const writes = jest.spyOn(store.stmtInsertData, 'run');
        expect(store.getDataCacheStats().hitRate).toBeNull();
        const id = store.dataId('hello');
        expect(typeof id).toBe('number');
        expect(store.dataId('hello')).toBe(id);
        expect(reads).toHaveBeenCalledTimes(1);
        expect(writes).toHaveBeenCalledTimes(1);
        expect(store.getDataCacheStats()).toEqual({
            hits: 1, misses: 1, hitRate: 0.5, itemCount: 1,
            keyCodeUnits: 5, maxKeyCodeUnits: 50000000, utf16PayloadBytes: 10,
        });
        store.getDataCacheStats();
        expect(reads).toHaveBeenCalledTimes(1);
        expect(writes).toHaveBeenCalledTimes(1);
    });

    test('reports UTF-16 code units rather than UTF-8 bytes or resident memory', () => {
        for (const key of ['ascii', 'é', '漢', '🙂']) store.dataId(key);
        const stats = store.getDataCacheStats();
        expect(stats.keyCodeUnits).toBe(9);
        expect(stats.utf16PayloadBytes).toBe(18);
        expect(stats.itemCount).toBe(4);
        const types = [];
        store.dataCache.forEach((value, key) => types.push([typeof key, typeof value]));
        expect(types).toEqual(Array(4).fill(['string', 'number']));
    });

    test('repeated data and unique message tags have measurable effectiveness', () => {
        const reads = jest.spyOn(store.stmtGetExistingDataId, 'get');
        for (let n = 0; n < 2000; n++) {
            for (const key of ['#shared', `text-${n % 20}`, JSON.stringify({ time: n, msgid: `${n}` }),
                `nick-${n % 100}`, '#shared']) store.dataId(key);
        }
        const stats = store.getDataCacheStats();
        expect(stats.misses).toBe(2121);
        expect(stats.hits).toBe(7879);
        expect(stats.hitRate).toBeCloseTo(0.7879);
        expect(reads).toHaveBeenCalledTimes(stats.misses);
        expect(stats.itemCount).toBe(2121);
    });

    test('eviction and orphan invalidation retain their existing database semantics', () => {
        store.dataCache.max = 5;
        const first = store.dataId('first');
        store.dataId('other');
        expect(store.dataCache.get('first')).toBeUndefined();
        expect(store.dataId('first')).toBe(first);
        store.cleanupOrphanedData([first]);
        expect(store.dataCache.get('first')).toBeUndefined();
        const reinserted = store.dataId('first');
        expect(store.db.prepare('SELECT id FROM data WHERE data = ?').get('first').id).toBe(reinserted);
        expect(store.getDataCacheStats().misses).toBe(4);
    });

    const cacheLogs = () => global.l.info.mock.calls
        .map(([message]) => message).filter(message => message.startsWith('SQLite dataCache:'));

    function expiredChannel() {
        store.retentionDaysChannels = 30;
        const buffer = store.dataId('#old');
        const prefix = store.dataId('nick');
        store.dataId('nick');
        const insert = store.db.prepare(`INSERT INTO logs (user_id, network_id, bufferref, time, prefixref)
            VALUES (1, 1, ?, ?, ?)`);
        for (let n = 0; n < 7; n++) insert.run(buffer, Date.now() - 100 * 86400000, prefix);
    }

    test('logs once after a multi-page cycle, preserving cumulative counters across cycles', async () => {
        expiredChannel();
        const snapshot = jest.spyOn(store, 'getDataCacheStats');
        const vacuum = jest.spyOn(store, 'runVacuum');
        const options = { batchSize: 2, pause: () => Promise.resolve(), throwOnError: true };
        expect((await store.runRetentionCleanup(options)).channels).toBe(7);
        expect(snapshot).toHaveBeenCalledTimes(1);
        expect(vacuum.mock.invocationCallOrder[0]).toBeLessThan(snapshot.mock.invocationCallOrder[0]);
        expect(cacheLogs()).toEqual([
            'SQLite dataCache: 0 entries, 1 hits, 2 misses, hit rate 33.3%, 0/50000000 key units',
        ]);
        await store.runRetentionCleanup(options);
        expect(snapshot).toHaveBeenCalledTimes(2);
        expect(cacheLogs()).toHaveLength(2);
        expect(cacheLogs()[1]).toBe(cacheLogs()[0]);
        expect(store.dataCacheHits).toBe(1);
        expect(store.dataCacheMisses).toBe(2);
    });

    test('logs N/A for a completed cycle without cache lookups', async () => {
        await store.runRetentionCleanup({ throwOnError: true });
        expect(cacheLogs()).toEqual([
            'SQLite dataCache: 0 entries, 0 hits, 0 misses, hit rate N/A, 0/50000000 key units',
        ]);
    });

    test('does not emit the summary when retention fails and rolls back', async () => {
        expiredChannel();
        const snapshot = jest.spyOn(store, 'getDataCacheStats');
        jest.spyOn(store, 'runDataCleanup').mockImplementation(() => { throw new Error('cleanup failed'); });
        await expect(store.runRetentionCleanup({ throwOnError: true })).rejects.toThrow('cleanup failed');
        expect(cacheLogs()).toHaveLength(0);
        expect(snapshot).not.toHaveBeenCalled();
        expect(store.db.prepare('SELECT count(*) AS n FROM logs').get().n).toBe(7);
    });

    test('does not emit the summary if the final maintenance step rejects', async () => {
        const snapshot = jest.spyOn(store, 'getDataCacheStats');
        jest.spyOn(store, 'runVacuum').mockRejectedValue(new Error('maintenance failed'));
        await expect(store.runRetentionCleanup({ throwOnError: true })).rejects.toThrow('maintenance failed');
        expect(cacheLogs()).toHaveLength(0);
        expect(snapshot).not.toHaveBeenCalled();
    });
});
