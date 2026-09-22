const IrcMessage = require('irc-framework').Message;

let baseId = 'kiwibnc-'+Date.now();
let msgId = 0;

// TODO(labeled-response): an end-to-end labeled-response relay was implemented on
// this extension to let clients correlate their own message echoes by label
// (instead of the heuristic nick+msgid+content match the client does today), then
// reverted on 2026-07-17.
//
// Why reverted: InspIRCd does NOT put the label back on echo-message echoes —
// verified against InspIRCd-3 (Europnet, the production target) AND a fresh
// InspIRCd-4 (irc.teranova.net). Both ACK `labeled-response` in CAP yet echo
// PRIVMSG/NOTICE back with `inspircd.org/echo` + msgid but no `label` and no ACK.
// Their labeledresponse module labels command *replies*, not echoes (the echo is
// emitted on a different code path). Per the IRCv3 spec, channel-message echoes
// MUST carry the label, so this is an InspIRCd non-compliance, not a config issue.
// Only Ergo/Oragono-family servers return the label correctly. On any InspIRCd
// network the relay was permanently inert AND skipped the clientcommands fan-out,
// which lost the sender's message for secondary clients lacking echo-message.
//
// Revisit when we target a spec-compliant upstream (Ergo) or InspIRCd fixes it: the
// full implementation (label namespacing, TTL map, batch handling, upstream-id
// ownership checks, runtime-detection design notes) lives in git history on branch
// feat/labeled-response-echo, commits 2b38472 / 8ad3ba0 / 60e76c3. The right shape
// on return is per-upstream runtime detection defaulting to the safe path (no
// fan-out skip) and enabling the labeled path only once a label round-trip is
// observed on that connection, so non-compliant servers never regress.

module.exports.init = async function init(hooks) {
    // echo-message support
    hooks.on('available_caps', (event) => {
        event.caps.add('echo-message');
    });
    hooks.on('wanted_caps', (event) => {
        event.wantedCaps.add('echo-message');
    });
    hooks.on('message_from_client', (event) => {
        if (!event.client.state.netRegistered) {
            return;
        }
        let upstream = event.client.upstream;
        if(!upstream) {
            return;
        }
        // If the server doesn't support echo-message we do it ourselves with our own message.
        if (!upstream.state.caps.has('echo-message')) {
            let msg = event.message;
            if(msg.command !== 'PRIVMSG' && msg.command !== 'NOTICE' && msg.command !== 'TAGMSG') {
                return;
            }
            // Give ID to original message so it is stored correctly
            msg.tags.msgid = baseId + '-' + msgId++;

            let m = new IrcMessage(msg.command, ...msg.params);
            m.tags = {...msg.tags};
            m.nick = upstream.state.nick;
            m.username = upstream.state.username;
            m.hostname = upstream.state.host;
            m.prefix = m.nick + '!' + m.username + '@' + m.hostname;

            upstream.forEachClient((client) => {
                // Don't echo back to client that sent if it's not expecting it.
                if(client === event.client && !event.client.state.caps.has('echo-message')) {
                    return;
                }
                client.writeMsg(m);
            });
        }
    });
    hooks.on('message_to_client', (event) => {
        // Disables normal bnc behavior of echoing a message to connected clients
        if(!event.client.upstream) {
            return;
        }
        let {client, message} = event;
        if(message.command === 'PRIVMSG' || message.command === 'NOTICE' || message.command === 'TAGMSG') {
            if (!client.state.caps.has('echo-message')
            && (client.upstream.state.nick || '').toLowerCase() === (message.nick || '').toLowerCase()) {
                event.preventDefault();
            } else if(client.state.caps.has('echo-message') && message.source === 'client') {
                event.preventDefault(); // Client and server support echo-message and msg came from a client, so ignore it.
            }
        }
    });
};
