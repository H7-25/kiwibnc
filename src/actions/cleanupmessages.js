const sqlite3 = require('better-sqlite3');

/**
 * Runs message retention in a separate Node.js process.
 *
 * Only rows from the logs table are deleted here. Orphaned rows from
 * the data table will be handled separately during offline maintenance,
 * because the live KiwiBNC process keeps data IDs in an in-memory cache.
 */
module.exports = async function cleanupMessages() {
    const app = await require('../libs/bootstrap')('cleanupmessages');
    const loggingConf = app.conf.get('logging', {});

    if (!loggingConf.database) {
        l.error('Missing logging.database in config');
        process.exitCode = 1;
        return;
    }

    const databasePath = app.conf.relativePath(
        loggingConf.database
    );

    const retentionDaysChannels = parseNonNegativeInteger(
        loggingConf.retention_days_channels,
        0
    );

    const retentionDaysPMs = parseNonNegativeInteger(
        loggingConf.retention_days_pms,
        0
    );

    const batchSize = parsePositiveInteger(
        loggingConf.retention_cleanup_batch_size,
        1000
    );

    /*
     * Global limit shared by channel and PM cleanup.
     * Zero means unlimited.
     */
    const maxRows = parseNonNegativeInteger(
        loggingConf.retention_cleanup_max_rows,
        100000
    );

    /*
     * Pause between transactions to reduce contention with
     * the live KiwiBNC process.
     */
    const pauseMs = parseNonNegativeInteger(
        loggingConf.retention_cleanup_pause_ms,
        100
    );

    if (
        retentionDaysChannels === 0 &&
        retentionDaysPMs === 0
    ) {
        l.info('Message retention is disabled');
        return;
    }

    let db = null;

    try {
        db = new sqlite3(databasePath);

        /*
         * KiwiBNC uses WAL too. The cleanup process opens its own
         * independent SQLite connection.
         */
        db.pragma('journal_mode = WAL');
        db.pragma('busy_timeout = 10000');

        /*
         * Required because retention searches globally by timestamp.
         */
        db.exec(`
            CREATE INDEX IF NOT EXISTS logs_time
            ON logs (time)
        `);

        const deleteChannelBatch = db.prepare(`
            DELETE FROM logs
            WHERE rowid IN (
                SELECT logs.rowid
                FROM logs
                INNER JOIN data AS buffer
                    ON buffer.id = logs.bufferref
                WHERE logs.time < ?
                  AND (
                      buffer.data LIKE '#%'
                      OR buffer.data LIKE '&%'
                  )
                ORDER BY logs.time ASC
                LIMIT ?
            )
        `);

        const deletePmBatch = db.prepare(`
            DELETE FROM logs
            WHERE rowid IN (
                SELECT logs.rowid
                FROM logs
                INNER JOIN data AS buffer
                    ON buffer.id = logs.bufferref
                WHERE logs.time < ?
                  AND buffer.data NOT LIKE '#%'
                  AND buffer.data NOT LIKE '&%'
                ORDER BY logs.time ASC
                LIMIT ?
            )
        `);

        const state = {
            totalDeleted: 0,
            startedAt: Date.now(),
        };

        await processRetention({
            db,
            statement: deleteChannelBatch,
            label: 'channels',
            days: retentionDaysChannels,
            batchSize,
            maxRows,
            pauseMs,
            state,
        });

        if (
            maxRows === 0 ||
            state.totalDeleted < maxRows
        ) {
            await processRetention({
                db,
                statement: deletePmBatch,
                label: 'PMs',
                days: retentionDaysPMs,
                batchSize,
                maxRows,
                pauseMs,
                state,
            });
        }

        const durationMs = Date.now() - state.startedAt;

        l.info(
            `Message retention completed: ` +
            `${state.totalDeleted} rows removed in ${durationMs} ms`
        );
    } catch (err) {
        l.error('Message retention failed', err);
        process.exitCode = 1;
    } finally {
        if (db) {
            try {
                db.close();
            } catch (err) {
                l.error('Error closing retention database', err);
                process.exitCode = 1;
            }
        }
    }
};

/**
 * Processes one retention category using short transactions.
 */
async function processRetention(options) {
    const {
        db,
        statement,
        label,
        days,
        batchSize,
        maxRows,
        pauseMs,
        state,
    } = options;

    if (days <= 0) {
        return;
    }

    const cutoffTime =
        Date.now() - (days * 24 * 60 * 60 * 1000);

    l.info(
        `Starting ${label} retention: ` +
        `${days} days, batch=${batchSize}`
    );

    while (true) {
        let currentBatchSize = batchSize;

        if (maxRows > 0) {
            const remaining =
                maxRows - state.totalDeleted;

            if (remaining <= 0) {
                l.info(
                    `Global cleanup limit reached ` +
                    `(${state.totalDeleted} rows)`
                );
                return;
            }

            currentBatchSize = Math.min(
                currentBatchSize,
                remaining
            );
        }

        let info;

        try {
            /*
             * Each batch is committed independently, keeping SQLite
             * write locks shorter while KiwiBNC is running.
             */
            info = db.transaction(() => {
                return statement.run(
                    cutoffTime,
                    currentBatchSize
                );
            })();
        } catch (err) {
            if (
                err.code === 'SQLITE_BUSY' ||
                err.code === 'SQLITE_LOCKED'
            ) {
                l.warn(
                    'SQLite is busy; retrying in one second'
                );

                await sleep(1000);
                continue;
            }

            throw err;
        }

        const deleted = info.changes;
        state.totalDeleted += deleted;

        if (
            state.totalDeleted % 100000 < deleted ||
            deleted < currentBatchSize
        ) {
            l.info(
                `Message retention removed ` +
                `${state.totalDeleted} rows so far`
            );
        }

        /*
         * A partial batch means there are no more matching rows
         * for this category.
         */
        if (deleted < currentBatchSize) {
            l.info(`${label} retention completed`);
            return;
        }

        if (
            maxRows > 0 &&
            state.totalDeleted >= maxRows
        ) {
            l.info(
                `Global cleanup limit reached ` +
                `(${state.totalDeleted} rows)`
            );
            return;
        }

        await sleep(pauseMs);
    }
}

function parseNonNegativeInteger(value, defaultValue) {
    const parsed = Number(value);

    if (
        Number.isFinite(parsed) &&
        parsed >= 0
    ) {
        return Math.floor(parsed);
    }

    return defaultValue;
}

function parsePositiveInteger(value, defaultValue) {
    const parsed = Number(value);

    if (
        Number.isFinite(parsed) &&
        parsed > 0
    ) {
        return Math.floor(parsed);
    }

    return defaultValue;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}