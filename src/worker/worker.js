const fs = require('fs');
const { v4: uuidv4 } = require('uuid');
const { ircLineParser } = require('irc-framework');
const Koa = require('koa');
const koaStatic = require('koa-static');
const KoaRouter = require('@koa/router');
const KoaMount = require('koa-mount');
const { koaBody } = require('koa-body');
const IpCidr = require("ip-cidr");
const MessageStores = require('./messagestores/');
const ConnectionOutgoing = require('./connectionoutgoing');
const ConnectionIncoming = require('./connectionincoming');
const ConnectionDict = require('./connectiondict');
const hooks = require('./hooks');
const { parseBindString, now } = require('../libs/helpers');

async function run() {
    let app = await require('../libs/bootstrap')('worker');
    await app.initQueue('worker');
    app.queue.sendToSockets('config.reload');
    global.config = app.conf;

    try {
        await initDatabase(app);
    } catch (err) {
        l.error('Error connecting to the database.', err.message);
        process.exit(1);
    }

    app.messages = new MessageStores(app.conf);
    await app.messages.init();

    // Container for all connection instances
    app.cons = new ConnectionDict(app.db, app.userDb, app.messages, app.queue);

    // A rejected promise that nobody awaited would otherwise kill the worker. As the queue
    // redelivers whatever message was in flight, that turns a single bad message into a crash
    // loop that hammers everybody and backs the queue up. Log it loudly and stay alive instead.
    process.on('unhandledRejection', (reason) => {
        let stack = (reason && reason.stack) || reason;
        l.error(`### Unhandled rejection - this should not happen but the worker is still running. ${stack}`);
    });

    app.prepareShutdown = () => prepareShutdown(app);
    process.on('SIGQUIT', () => {
        app.prepareShutdown();
    });
    process.on('SIGTERM', () => {
        app.prepareShutdown();
    });

    // process.send only exists if we were forked from the sockets layer. Hijack SIGINT so
    // that ctrl+c in kiwibnc doesn't do anything to us. Sockets layer will send SIGQUIT to
    // us if needed.
    if (process.send) {
        process.on('SIGINT', () => {
            // noop
        });
    }


    initWebserver(app);
    initStatus(app);
    await initExtensions(app);
    broadcastStats(app);
    monitorEventLoop(app);
    await startServers(app);
    await loadConnections(app);

    // Now that all the connection states have been laoded, start accepting events for them
    listenToQueue(app);

    // Reconcile reloaded incoming connections against the sockets layer, reaping any whose
    // socket did not survive our restart.
    reconcileIncomingConnections(app);

    return app;
}

// After a worker-only restart the sockets layer keeps client sockets alive, so loadConnections()
// reloads every incoming connection row. But rows whose socket died while we were down never
// receive a connection.close, so they would linger forever as zombies that still get forwarded
// messages (inflating linkedIncomingConIds and duplicating traffic). Ask the sockets layer which
// connections are actually alive; mark survivors connected and purge the rest. The reply is
// handled by the 'connections.active' listener registered in listenToQueue().
function reconcileIncomingConnections(app) {
    let pending = app.reloadedIncomingConIds;
    if (!pending || pending.size === 0) {
        return;
    }

    l.info(`Reconciling ${pending.size} reloaded incoming connections against the sockets layer`);
    app.queue.sendToSockets('connections.getactive', {});

    // Fail-safe: if the sockets layer never answers (e.g. an older version without the
    // connections.getactive handler), do not reap anything — leaving a live connection alone
    // is safer than wrongly killing it.
    app.reconcileIncomingTimer = setTimeout(() => {
        if (app.reloadedIncomingConIds && app.reloadedIncomingConIds.size > 0) {
            l.warn(`Incoming connection reconciliation timed out; leaving ${app.reloadedIncomingConIds.size} connections untouched`);
        }
        app.reloadedIncomingConIds = null;
    }, 30000);
}

async function initExtensions(app) {
    let extensions = app.conf.get('extensions.loaded') || [];
    for(let i=0; i<extensions.length; i++){
        let extName = extensions[i];
        try {
            let extPath = (extName[0] === '.' || extName[0] === '/') ?
                app.conf.relativePath(extName) :
                `../extensions/${extName}/`;

            l.info('Loading extension ' + extPath);
            let ext = require(extPath);
            if (ext && typeof ext.init === 'function') {
                await ext.init(hooks, app);
            }
        } catch (err) {
            l.error('Error loading extension ' + extName + ': ', err.stack);
        }
    }

    // Extensions can add their hooks before the builtin hooks so that they have
    // a chance to override any if they need
    hooks.addBuiltInHooks();
};

function broadcastStats(app) {
    function broadcast() {
        app.stats.gauge('stats.connections', app.cons.map.size);

        let mem = process.memoryUsage();
        app.stats.gauge('stats.memoryheapused', mem.heapUsed);
        app.stats.gauge('stats.memoryheaptotal', mem.heapTotal);
        app.stats.gauge('stats.memoryrss', mem.rss);

        fs.readdir('/proc/self/fd', (err, list) => {
            // Expected errors on OSs without /proc/
            if (err) return;
            app.stats.gauge('stats.fdcount', list.length);
        });

        setTimeout(broadcast, 10000);
    }

    broadcast();
}

function monitorEventLoop(app) {
    let lastCheck = Date.now();
    const interval = 1000;

    setInterval(() => {
        const now = Date.now();
        const lag = now - lastCheck - interval;
        lastCheck = now;

        // Record lag in milliseconds
        // A lag > 20-50ms indicates CPU saturation
        app.stats.gauge('worker.eventloop_lag', Math.max(0, lag));
    }, interval);
}

async function prepareShutdown(app) {
    // This worker will get restarted by the sockets process automatically
    l.info('Gracefully shutting down...');

    // Flush all dirty connection states before exiting
    const savePromises = [];
    app.cons.map.forEach((con) => {
        if (con.state._dirty) {
            if (con.state._saveTimer) {
                clearTimeout(con.state._saveTimer);
                con.state._saveTimer = null;
            }
            con.state._dirty = false;
            savePromises.push(con.state.save());
        }
    });

    if (savePromises.length > 0) {
        l.info(`Flushing ${savePromises.length} dirty connection states...`);
        await Promise.all(savePromises);
    }

    await app.queue.stopListening();
    process.exit();
}

function listenToQueue(app) {
    let cons = app.cons;
    app.queue.listenForEvents();

    app.queue.on('reset', async (event) => {
        l.info('Sockets server was reset, flushing all connections');

        // Wipe out all incoming connection states. Incoming connections need to manually reconnect
        await app.db.dbConnections.raw('DELETE FROM connections WHERE type = ?', [ConnectionDict.TYPE_INCOMING]);

        // Since there are now no incoming connections, clear all incoming<>outgoing links
        await app.db.dbConnections.raw(`UPDATE connections SET linked_con_ids = '[]'`);

        // If we don't have any connections then we have nothing to clear out. We do
        // need to start our servers again though
        if (cons.size === 0) {
            startServers(app);
            return;
        }

        app.prepareShutdown();
    });

    // When the socket layer accepts a new incoming connection
    app.queue.on('connection.new', async (event) => {
        // If we have an origin from a websocket, make sure we have it whitelisted
        let origins = app.conf.get('listeners.websocket_origins', []);
        if (origins && origins.length > 0 && event.origin) {
            let foundOrigin = origins.find(o => (
                o.toLowerCase() === event.origin.toLowerCase()
            ));

            if (!foundOrigin) {
                l.error('Incoming connection from unknown origin.', event.origin);
                app.queue.sendToSockets('connection.close', {id: event.id});
                return;
            }
        }

        l.debug('New incoming connection', event.id);
        let c = await app.cons.loadFromId(event.id, ConnectionDict.TYPE_INCOMING);
        c.state.host = event.host;
        c.state.port = event.port;

        try {
            await c.state.save();
        } catch (err) {
            l.error('Error saving incoming connection.', err.message);
            app.queue.sendToSockets('connection.close', {id: c.id});
            c.destroy();
            return;
        }

        await c.onAccepted();
    });

    // When the socket layer has opened a new outgoing connection
    app.queue.on('connection.open', async (event) => {
        let con = cons.get(event.id);
        if (con) {
            await con.onUpstreamConnected();
        }
    });

    // When the socket layer reports a connection is already active (worker restart scenario)
    app.queue.on('connection.existing', async (event) => {
        let con = cons.get(event.id);
        if (con && con instanceof ConnectionOutgoing) {
            l.info(`Connection ${event.id} already active in sockets, restoring state`);
            con.state.connected = true;
            con.state.netRegistered = true;
            con.state.receivedMotd = true;
            con.state.markDirty();

            // Socket survived our restart so CAP was never renegotiated; re-fetch
            // the authoritative list in case our reloaded caps are stale.
            con.writeLine('CAP', 'LIST');
        }
    });
    // Reply to reconcileIncomingConnections(): the sockets layer tells us which connection ids
    // are actually alive. Mark reloaded incoming survivors as connected and reap the zombies.
    app.queue.on('connections.active', async (event) => {
        let pending = app.reloadedIncomingConIds;
        if (!pending) {
            return;
        }
        if (app.reconcileIncomingTimer) {
            clearTimeout(app.reconcileIncomingTimer);
            app.reconcileIncomingTimer = null;
        }

        let liveIds = new Set(event.ids || []);
        let reaped = 0;
        for (const id of pending) {
            let con = cons.get(id);
            if (!con || !(con instanceof ConnectionIncoming)) {
                continue;
            }

            if (liveIds.has(id)) {
                // Socket survived our restart — restore the connected flag that the reload lost.
                con.state.connected = true;
                con.state.markDirty();
            } else {
                // Socket is gone. destroy() unlinks from the upstream, removes it from the
                // connection dict and deletes its persisted row.
                l.info(`Reaping stale incoming connection ${id} (socket not alive in sockets layer)`);
                con.destroy();
                reaped++;
            }
        }

        l.info(`Incoming reconciliation done: reaped ${reaped} stale connection(s)`);
        app.reloadedIncomingConIds = null;
    });

    app.queue.on('connection.error', async (event) => {
        l.error(`Server error ${event.id} ${event.error.message}`);
    });
    app.queue.on('connection.close', async (event) => {
        if (event.error) {
            l.debug(`Connection ${event.id} closed. Error: ${event.error.code}`);
        } else {
            l.debug(`Connection ${event.id} closed.`);
        }

        let con = cons.get(event.id);
        try {
            if (con && con instanceof ConnectionOutgoing) {
                await con.onUpstreamClosed(event.error);
            } else if (con && con instanceof ConnectionIncoming) {
                await con.onClientClosed(event.error);
            }
        } catch (err) {
            // A rejection here would otherwise vanish silently into the queue handler
            l.error(`Error closing connection ${event.id}:`, err.stack || err.message);
        }
    });
    app.queue.on('connection.data', async (event) => {
        let timer = app.stats.timerStart('worker.process_message_time');
        let con = cons.get(event.id);
        if (!con) {
            l.warn('Recieved data for unknown connection ' + event.id);
            timer.stop();
            return;
        }

        let msg = ircLineParser(event.data);
        if (!msg) {
            let snippet = event.data.substr(0, 300);
            if (event.data.length > 300) {
                snippet += '...';
            }
            l.warn('Recieved malformed IRC line from connection ' + event.id + ' - ' + snippet);
            timer.stop();
            return;
        }

        if (con instanceof ConnectionIncoming) {
            await con.messageFromClient(msg, event.data);
        } else {
            await con.messageFromUpstream(msg, event.data);
        }
        timer.stop();
    });

    // Handle batched data from sockets (more efficient than individual messages)
    app.queue.on('connection.data.batch', async (event) => {
        let con = cons.get(event.id);
        if (!con) {
            l.warn('Recieved batch data for unknown connection ' + event.id);
            return;
        }

        for (const line of event.lines) {
            let msg = ircLineParser(line);
            if (!msg) {
                let snippet = line.substr(0, 300);
                if (line.length > 300) {
                    snippet += '...';
                }
                l.warn('Recieved malformed IRC line from connection ' + event.id + ' - ' + snippet);
                continue;
            }

            if (con instanceof ConnectionIncoming) {
                await con.messageFromClient(msg, line);
            } else {
                await con.messageFromUpstream(msg, line);
            }
        }
    });
}

// Start any listening servers on interfaces specified in the config
async function startServers(app) {
    // Close all existing listeners first
    app.queue.sendToSockets('listeners.closeall');

    let binds = app.conf.get('listeners.bind', []);
    for (let i = 0; i < binds.length; i++) {
        let parts = parseBindString(binds[i]);
        if (!parts) {
            l.error('Invalid listening server type, ' + binds[i]);
            return;
        }

        let host = parts.hostname || '0.0.0.0';
        let port = parseInt(parts.port || '6667', 10);
        let type = (parts.proto || 'tcp').toLowerCase();

        // Treat 'ssl' as an alias to 'tls'
        if (type === 'ssl') {
            type = 'tls';
        }

        let listenOpts = {
            host: host,
            port: port,
            type: type,
            id: uuidv4(),
        };

        // Add any TLS certs and keys
        if (type === 'tls') {
            let listeners = app.conf.get('listeners');
            if (!listeners.tls_key || !listeners.tls_cert) {
                l.error('A TLS listener requires the tls_key and tls_cert config options set');
                continue;
            }

            try {
                listenOpts.key = fs.readFileSync(app.conf.relativePath(listeners.tls_key), 'utf8');
                listenOpts.cert = fs.readFileSync(app.conf.relativePath(listeners.tls_cert), 'utf8');
            } catch (err) {
                l.error('Error reading TLS key or certifcate', err.message);
                continue;
            }

            if (!listenOpts.key || !listenOpts.cert) {
                l.error('A TLS listener requires a valid key and certificate');
                continue;
            }
        }

        app.queue.sendToSockets('connection.listen', listenOpts);
    }
}

// loadConnections() open()s every outgoing row, but they accumulate cruft:
// orphans from deleted networks, plus duplicates and stale connected=1 flags from
// crashes. Keep at most one row per existing network and reset connected before
// opening anything, so dead connections aren't resurrected.
async function reconcileOutgoingConnections(app) {
    let netRows = await app.db.dbUsers('user_networks').select('id');
    let validNetworkIds = new Set(netRows.map((r) => Number(r.id)));

    // connected is read before the reset below so the dedup can prefer the live row.
    let rows = await app.db.dbConnections('connections')
        .where('type', ConnectionDict.TYPE_OUTGOING)
        .select('conid', 'auth_user_id', 'auth_network_id', 'connected', 'net_registered', 'last_statesave');

    // Group rows sharing the same user + network.
    let groups = new Map();
    for (const row of rows) {
        let key = row.auth_user_id + ':' + row.auth_network_id;
        if (!groups.has(key)) {
            groups.set(key, []);
        }
        groups.get(key).push(row);
    }

    let toDelete = [];
    for (const group of groups.values()) {
        let networkId = Number(group[0].auth_network_id);

        // Orphan: the network no longer exists in the user db. Drop every row.
        if (!networkId || !validNetworkIds.has(networkId)) {
            toDelete.push(...group.map((r) => r.conid));
            continue;
        }

        // Duplicate: keep the best row, delete the rest.
        if (group.length > 1) {
            group.sort(compareOutgoingRows);
            toDelete.push(...group.slice(1).map((r) => r.conid));
        }
    }

    if (toDelete.length > 0) {
        l.info(`Reconciling outgoing connections: removing ${toDelete.length} orphan/duplicate row(s)`);
        // Chunk to stay well under SQLite's bound-parameter limit.
        const CHUNK = 400;
        for (let i = 0; i < toDelete.length; i += CHUNK) {
            await app.db.dbConnections('connections')
                .whereIn('conid', toDelete.slice(i, i + CHUNK))
                .delete();
        }
    }

    // Nothing is connected until a live socket reconfirms it on open(), so a flag
    // stuck at 1 by a crash can't be trusted.
    await app.db.dbConnections('connections')
        .where('type', ConnectionDict.TYPE_OUTGOING)
        .update({ connected: 0 });

    // Incoming rows keep pointing at their network after it's deleted. Any later
    // makeUpstream() for them would look up a network that isn't there, so detach
    // them and let the client re-attach to something that exists.
    let incomingRows = await app.db.dbConnections('connections')
        .where('type', ConnectionDict.TYPE_INCOMING)
        .whereNot('auth_network_id', 0)
        .select('conid', 'auth_network_id');

    let orphanIncoming = incomingRows
        .filter((r) => !validNetworkIds.has(Number(r.auth_network_id)))
        .map((r) => r.conid);

    if (orphanIncoming.length > 0) {
        l.info(`Reconciling incoming connections: detaching ${orphanIncoming.length} row(s) from deleted networks`);
        const CHUNK = 400;
        for (let i = 0; i < orphanIncoming.length; i += CHUNK) {
            await app.db.dbConnections('connections')
                .whereIn('conid', orphanIncoming.slice(i, i + CHUNK))
                .update({ auth_network_id: 0 });
        }
    }
}

// Prefer the row the live socket is keyed to. We can't query the sockets layer,
// so connected (read before the reset) is the closest signal, then registered,
// then most recently saved.
function compareOutgoingRows(a, b) {
    let byConnected = (b.connected ? 1 : 0) - (a.connected ? 1 : 0);
    if (byConnected !== 0) {
        return byConnected;
    }

    let byRegistered = (b.net_registered ? 1 : 0) - (a.net_registered ? 1 : 0);
    if (byRegistered !== 0) {
        return byRegistered;
    }

    return (b.last_statesave || 0) - (a.last_statesave || 0);
}

async function loadConnections(app) {
    await reconcileOutgoingConnections(app);

    let rows = await app.db.dbConnections.raw('SELECT conid, type, bind_host, auth_user_id FROM connections');
    l.info(`Loading ${rows.length} connections`);
    let types = ['OUTGOING', 'INCOMING', 'LISTENING'];
    // Track the incoming connections we reload so we can reconcile them against the
    // sockets layer once the queue is listening: any whose socket did not survive our
    // restart must be reaped, otherwise they linger as zombies (connected=0,
    // netRegistered=1) that still receive forwarded messages. See reconcileIncomingConnections().
    app.reloadedIncomingConIds = new Set();
    for (const row of rows) {
        l.debug(`connection ${row.conid} ${types[row.type]} ${row.bind_host}`);

        if (row.type === ConnectionDict.TYPE_INCOMING) {
            // Incoming connections that never authenticated are stale — the client
            // disconnected before completing registration and the state was never
            // cleaned up (typically because the worker crashed). Purge them now.
            if (!row.auth_user_id) {
                await app.db.dbConnections('connections').where('conid', row.conid).delete();
                continue;
            }
            await app.cons.loadFromId(row.conid, row.type);
            app.reloadedIncomingConIds.add(row.conid);
        } else if (row.type === ConnectionDict.TYPE_OUTGOING) {
            let con = await app.cons.loadFromId(row.conid, row.type);
            con.open();
        } else if (row.type === ConnectionDict.TYPE_LISTENING) {
            let parts = parseBindString(row.bind_host);
            if (!parts) {
                l.error('Invalid listening server type, ' + row.bind_host);
                continue;
            }
            let host = parts.hostname || '0.0.0.0';
            let port = parseInt(parts.port || '6667', 10);
            let type = (parts.proto || 'tcp').toLowerCase();

            app.queue.sendToSockets('connection.listen', {
                host: host,
                port: port,
                type: type,
                id: row.conid,
            });
        }
    }
}

// Connect to the database, logging warnings if it takes too long
async function initDatabase(app) {
    let dbConnectTmr = setInterval(() => {
        l.warn('Waiting for the database connection...');
    }, 5000);

    try {
        await app.initDatabase();
        clearInterval(dbConnectTmr);
    } catch (err) {
        clearInterval(dbConnectTmr);
        throw err;
    }
}

async function initWebserver(app) {
    let basePath = app.conf.get('webserver.base_path', '/')
        .replace(/\/$/, ''); // strip trailing slashes
    if (basePath && basePath[0] !== '/') {
        // Base path must always be absolute
        basePath = '/' + basePath;
    }

    app.webserver = new Koa();
    app.webserver.proxy = true;
    app.webserver.context.basePath = basePath;

    app.webserver.on('error', (error) => {
        if (error.code === 'EPIPE' || error.code === 'ECONNRESET') {
            // These errors are expected as clients always disconnect at random times before
            // waiting for a response, or general network issues
            return;
        } else if (error.status >= 400 && error.status < 500) {
            // A bad request the server already rejected, eg a scanner probing for path
            // traversal. Logging a stack per attempt lets anyone flood the log at will
            l.debug(`Webserver ${error.status} ${error.message}`);
        } else {
            l.error('Webserver error', error);
        }
    });

    let router = app.webserver.router = new KoaRouter({
        prefix: basePath,
    });

    app.webserver.use(koaBody({ multipart: true }));
    app.webserver.use(router.routes());
    app.webserver.use(router.allowedMethods());

    let staticServ = koaStatic(app.conf.relativePath(app.conf.get('webserver.public_dir', './public_http')));
    app.webserver.use(KoaMount(basePath || '/', staticServ));

    const defaultSockPath = process.platform === "win32" ?
        '\\\\.\\pipe\\kiwibnc_httpd.sock' :
        '/tmp/kiwibnc_httpd.sock';
    let sockPath = app.conf.get('webserver.bind_socket', defaultSockPath);

    if (app.conf.get('webserver.enabled') && sockPath) {
        try {
            // Make sure the socket doesn't already exist
            fs.unlinkSync(sockPath);
        } catch (err) {
        }

        app.webserver.listen(sockPath);
        l.debug(`Webserver running`);
    }
}

async function initStatus(app) {
    if (!app.conf.get('webserver.status_enabled', true)) {
        return;
    }

    // Trim any trailing slashes from the status URL path
    const statusPath = app.conf.get('webserver.status_path', '/status').replace(/\/$/, '');
    const router = app.webserver.router;

    router.get('status', statusPath, statusAuth, async (ctx) => {
        ctx.body = '<a href="connections">Connections</a>';
    });

    router.get('status', statusPath + '/connections', statusAuth, async (ctx) => {
        const cons = app.cons;
        const conTypes = ['outgoing', 'incoming', 'server'];
        ctx.response.body = '';
        cons.map.forEach((con, key) => {
            const columns = [];
            columns.push(key);
            columns.push(conTypes[con.state.type]);
            columns.push(con.state.host + ':' + con.state.port);
            columns.push(con.state.authUserId);
            ctx.response.body += columns.join(',') + '\n';
        });

        ctx.response.status = 200;
    });

    router.get('status.con', statusPath + '/connections/:con_id', statusAuth, async (ctx) => {
        const cons = app.cons;
        const con = cons.get(ctx.params.con_id);
        if (!con) {
            ctx.response.status = 404;
            return;
        }

        const ignoreKeys = ['db'];
        const lastKeys = ['isupports', 'registrationLines'];
        const data = Object.create(null);
        Object.entries(con.state).forEach(([k, v]) => {
            if (ignoreKeys.includes(k)) {
                return;
            }
            if (k === 'sasl') {
                data.sasl = {}
                data.sasl.account = v.account;
                // Don't reveal the password, just a bool if there is one
                data.sasl.password = v.password !== '';
                return;
            }
            if (k === 'password') {
                // Don't reveal the password, just a bool if there is one
                data.password = v !== '';
                return;
            }
            data[k] = v;
        });

        ctx.response.body = JSON.stringify(data, Object.keys(data).sort(
            // Sort so lastKeys are at the end to make it more user readable
            (a, b) => {
                if (lastKeys.includes(a) && lastKeys.includes(b)) {
                    return lastKeys.indexOf(a) > lastKeys.indexOf(b) ? 1 : -1;
                }
                if (lastKeys.includes(a)) {
                    return 1;
                }
                if (lastKeys.includes(b)) {
                    return -1;
                }
                return a.localeCompare(b);
            }
        ), 4);
        ctx.response.status = 200;
    });
}

async function statusAuth(ctx, next, role, redirect) {
    const allowed = global.config.get('webserver.status_allowed_hosts', ['127.0.0.1/8']);

    for (let i = 0; i < allowed.length; i++) {
        if (!IpCidr.isValidAddress(allowed[i])) {
            l.error('CIDR is invalid:', allowed[i]);
            continue;
        }
        const cidr = new IpCidr(allowed[i]);
        if (cidr.contains(ctx.ip)) {
            return await next();
        }
    }
    ctx.response.status = 401;
}

module.exports = run();
