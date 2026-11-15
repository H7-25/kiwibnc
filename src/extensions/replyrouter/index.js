const ReplyRouter = require('./routes');

// Commands that are never "expected" replies - skip all processing for these
const PASSTHROUGH_COMMANDS = new Set([
    'PRIVMSG', 'NOTICE', 'JOIN', 'PART', 'QUIT', 'NICK', 'KICK', 'MODE',
    'TOPIC', 'INVITE', 'PING', 'PONG', 'ERROR', 'CAP', 'AUTHENTICATE',
    'TAGMSG', 'AWAY', 'BATCH', 'CHGHOST', 'SETNAME', 'ACCOUNT', 'FAIL',
    'WARN', 'NOTE'
]);

module.exports.init = function init(hooks) {
    hooks.on('message_to_clients', event => {
        let command = event.message.command.toUpperCase();

        // Fast path: common commands that are never expected replies
        // Skip all forEach loops and state lookups for these
        if (PASSTHROUGH_COMMANDS.has(command)) {
            return;
        }

        let clientsExpectingMsg = [];

        // Populate clientsExpectingMsg with clients expecting this command
        event.clients.forEach(client => {
            let expecting = client.state.tempGet('expecting_replies') || [];
    
            // Multiple outstanding requests must not duplicate the recipient.
            if (expecting.some(route => route.replies.some(reply => reply.cmd === command))) {
                clientsExpectingMsg.push(client);
            }
        });
    
        if (clientsExpectingMsg.length === 0) {
            // No specific clients are expecting this message so just let the message
            // go to them all
            return;
        }
    
        event.clients = clientsExpectingMsg;
        l.debug('Client was expecting this command,', command);
    
        // If this message is expected to be the last of its group, mark the client
        // as no longer expecting these type of messages again
        const EXPECTING_TIMEOUT = 30000; // 30 seconds
        const now = Date.now();

        event.clients.forEach(client => {
            let expecting = client.state.tempGet('expecting_replies') || [];

            // Use filter() instead of forEach+splice to avoid array mutation bugs
            // Also clean up stale entries that never received their ending reply
            expecting = expecting.filter(route => {
                let isEnding = route.replies.find(reply => reply.cmd === command && reply.ending);
                let isStale = (now - route.added) > EXPECTING_TIMEOUT;
                return !isEnding && !isStale;
            });

            client.state.tempSet('expecting_replies', expecting.length > 0 ? expecting : null);
        });
    });
    
    hooks.on('message_from_client', event => {
        let client = event.client;
        let msg = event.message;
    
        let expectReplies = ReplyRouter.expectedReplies(msg);
        if (!expectReplies) {
            return;
        }

        // NAMES with a target is answered by clientcommands locally, including
        // unknown buffers and disconnected networks. No upstream reply is due.
        if (msg.command.toUpperCase() === 'NAMES' && (msg.params[0] || '')) return;
        const upstream = client.upstream;
        if (!upstream || !upstream.state.connected) return;
    
        let expecting = client.state.tempGet('expecting_replies') || [];
        expecting.push({command: msg.command.toUpperCase(), replies: expectReplies, added: Date.now()});
        l.debug('Client now expecting one of', expectReplies.map(r=>r.cmd).join(' '));
        client.state.tempSet('expecting_replies', expecting);
    });

    // Bare NAMES is forwarded, but upstreamcommands consumes 353/366 and
    // synthesizes NAMES for clients. Its ending never reaches message_to_clients.
    hooks.on('message_from_upstream', event => {
        if (event.message.command !== '366') return;
        event.client.forEachClient(client => {
            const expecting = client.state.tempGet('expecting_replies');
            if (!expecting || !expecting.some(route => route.command === 'NAMES')) return;
            const remaining = expecting.filter(route => route.command !== 'NAMES');
            client.state.tempSet('expecting_replies', remaining.length ? remaining : null);
        });
    });

    // Replies to requests on the old IRC connection cannot arrive after close.
    hooks.on('connection_close', event => {
        event.upstream.forEachClient(client => {
            if (client.state.tempGet('expecting_replies')) {
                client.state.tempSet('expecting_replies', null);
            }
        });
    });
}
