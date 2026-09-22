const fs = require('fs');
const sqlite3 = require('better-sqlite3');

const MB = 1024 * 1024;

// Converts the message database to incremental auto_vacuum, and compacts it once.
//
// Deleting messages never shrinks a sqlite file on its own: the pages go on a freelist and get
// reused. Handing them back to the filesystem needs incremental auto_vacuum, and switching a
// database over to it can only be done by a full VACUUM, with the bouncer stopped.
//
// Rewriting in place needs as much free space again as the database itself, on the same disk.
// --into writes the compacted copy somewhere else instead, so it only needs room for the result.
module.exports = async function(options) {
    let app = await require('../libs/bootstrap')('vacuummessages');

    let loggingConf = app.conf.get('logging', {});
    if (!loggingConf.database) {
        l.error('No logging.database configured, nothing to vacuum');
        process.exit(1);
    }

    let dbPath = app.conf.relativePath(loggingConf.database);
    if (!fs.existsSync(dbPath)) {
        l.error(`Message database ${dbPath} does not exist`);
        process.exit(1);
    }

    let sizeBefore = fs.statSync(dbPath).size;
    let db = new sqlite3(dbPath);

    try {
        // Don't sit and wait on a running bouncer, tell the user about it instead
        db.pragma('busy_timeout = 2000');

        let autoVacuum = db.pragma('auto_vacuum', { simple: true });
        let pageSize = db.pragma('page_size', { simple: true });
        let freeBefore = db.pragma('freelist_count', { simple: true });

        l.info(`Database ${dbPath}`);
        l.info(`  size            ${(sizeBefore / MB).toFixed(1)}MB`);
        l.info(`  reclaimable     ${((freeBefore * pageSize) / MB).toFixed(1)}MB (${freeBefore} free pages)`);
        l.info(`  auto_vacuum     ${autoVacuum === 2 ? 'incremental' : autoVacuum === 1 ? 'full' : 'none'}`);

        let into = options && options.into;
        if (into) {
            if (fs.existsSync(into)) {
                l.error(`${into} already exists, refusing to overwrite it`);
                process.exit(1);
            }

            l.info(`Writing a compacted copy to ${into}`);
            // Only reads the source, so it needs room for the result and nothing more.
            // The copy comes out in incremental auto_vacuum mode whatever the source was.
            db.pragma('auto_vacuum = 2');
            db.exec(`VACUUM INTO '${into.replace(/'/g, "''")}'`);

            let counts = (conn) => ({
                logs: conn.prepare('SELECT count(*) c FROM logs').get().c,
                data: conn.prepare('SELECT count(*) c FROM data').get().c,
            });
            let sourceCounts = counts(db);

            let copy = new sqlite3(into, { readonly: true });
            let copyCounts;
            let check;
            let copyAutoVacuum;
            try {
                copyCounts = counts(copy);
                copyAutoVacuum = copy.pragma('auto_vacuum', { simple: true });
                check = copy.pragma('quick_check', { simple: true });
            } finally {
                copy.close();
            }

            let sizeCopy = fs.statSync(into).size;
            l.info(`  copy size       ${(sizeCopy / MB).toFixed(1)}MB`);
            l.info(`  messages        ${sourceCounts.logs} -> ${copyCounts.logs}`);
            l.info(`  data rows       ${sourceCounts.data} -> ${copyCounts.data}`);
            l.info(`  quick_check     ${check}`);
            l.info(`  auto_vacuum     ${copyAutoVacuum === 2 ? 'incremental' : copyAutoVacuum}`);

            if (check !== 'ok' || copyAutoVacuum !== 2 ||
                copyCounts.logs !== sourceCounts.logs || copyCounts.data !== sourceCounts.data)
            {
                l.error('The copy does not match the source, do not put it in place');
                process.exit(1);
            }

            l.info('Copy verified. With the bouncer stopped, put it in place with:');
            l.info(`  mv ${dbPath} ${dbPath}.old`);
            l.info(`  rm -f ${dbPath}-wal ${dbPath}-shm`);
            l.info(`  mv ${into} ${dbPath}`);
            l.info(`Keep ${dbPath}.old until the bouncer has restarted and served some history`);
            db.close();
            process.exit(0);
        }

        if (autoVacuum !== 2) {
            l.info('Converting to incremental auto_vacuum. This rewrites the whole database in');
            l.info('place: it needs as much free disk space again as the file above, on the same');
            l.info('disk, and can take a while. Use --into <path> to write the result elsewhere.');
            // On a database already in WAL mode, setting the pragma on its own is silently
            // ignored - only the VACUUM that follows actually converts it
            db.pragma('auto_vacuum = 2');
            db.exec('VACUUM');

            if (db.pragma('auto_vacuum', { simple: true }) !== 2) {
                l.error('Conversion failed: the database is still not in incremental auto_vacuum mode');
                process.exit(1);
            }
            l.info('Converted to incremental auto_vacuum');
        } else {
            // Already converted, just hand back whatever is currently free
            l.info('Reclaiming free pages');
            db.pragma('incremental_vacuum');
        }

        // The rewrite above went through the WAL, give that space back as well
        db.pragma('wal_checkpoint(TRUNCATE)');

        let sizeAfter = fs.statSync(dbPath).size;
        l.info(`Done: ${(sizeBefore / MB).toFixed(1)}MB -> ${(sizeAfter / MB).toFixed(1)}MB`);
        l.info('The bouncer will now reclaim space by itself after each retention pass');
    } catch (err) {
        if (err.code === 'SQLITE_BUSY') {
            l.error('The message database is in use. Stop the bouncer before running this');
        } else {
            l.error('Vacuum failed:', err.message);
        }
        process.exit(1);
    } finally {
        db.close();
    }

    process.exit(0);
}
