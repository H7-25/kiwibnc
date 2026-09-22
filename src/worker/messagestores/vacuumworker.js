const { parentPort, workerData } = require('worker_threads');
const sqlite3 = require('better-sqlite3');

/**
 * Reclaims free pages from the message database, off the main thread.
 *
 * SQLite only allows a single writer, so running this in a thread does not make the reclaim free
 * for everybody: while it holds the write lock, a write on the main thread waits. What it does buy
 * is that the main thread never burns CPU on it, and that readers (history lookups) are not
 * blocked at all in WAL mode. To keep the writes waiting for milliseconds rather than for the
 * whole reclaim, the work is done in small chunks with the lock released between each one.
 */

const {
    file,
    chunkPages,
    keepFreePages,
    pausePerChunk,
    busyTimeout,
    walSizeLimit,
} = workerData;

// Synchronous sleep. This thread has no event loop work to do, and unlike setTimeout it is
// guaranteed to yield the sqlite write lock for the full duration.
function sleep(ms) {
    if (ms > 0) {
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
    }
}

let db = null;
let pagesFreed = 0;

try {
    db = new sqlite3(file);
    db.pragma(`busy_timeout = ${busyTimeout}`);
    if (walSizeLimit >= 0) {
        db.pragma(`journal_size_limit = ${walSizeLimit}`);
    }

    const freelist = () => db.pragma('freelist_count', { simple: true });
    const pageSize = db.pragma('page_size', { simple: true });

    while (true) {
        let free = freelist();
        if (free <= keepFreePages) {
            break;
        }

        // Leave keepFreePages behind so that tomorrow's messages reuse the freelist instead of
        // growing the file again - reclaiming to the last page just means writing it back later
        let want = Math.min(chunkPages, free - keepFreePages);
        db.pragma(`incremental_vacuum(${want})`);

        let freedNow = free - freelist();
        pagesFreed += freedNow;
        if (freedNow <= 0) {
            // Nothing moved, the remaining free pages can't be reclaimed right now
            break;
        }

        sleep(pausePerChunk);
    }

    // Moving those pages was written to the WAL, so hand that space back too. TRUNCATE only
    // succeeds when no other connection is reading, so a busy result here is normal and expected -
    // journal_size_limit caps the file on the next checkpoint anyway.
    let checkpoint = db.pragma('wal_checkpoint(TRUNCATE)');
    let busy = !!(checkpoint && checkpoint[0] && checkpoint[0].busy);

    parentPort.postMessage({ ok: true, pagesFreed, pageSize, walTruncated: !busy });
} catch (err) {
    parentPort.postMessage({ ok: false, error: err.message, pagesFreed });
} finally {
    if (db) {
        try {
            db.close();
        } catch (err) {
            // Closing on the way out, nothing useful to do with a failure here
        }
    }
}
