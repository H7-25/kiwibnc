const Irc = require('irc-framework');
const { mParam, mParamU } = require('../../libs/helpers');

const MAX_MESSAGES = 50;

let stats = null;

module.exports.init = async function init(hooks, app) {
    stats = app.stats.makePrefix('chathistory');
    hooks.on('available_caps', event => {
        event.caps.add('draft/chathistory');
    });

    hooks.on('message_from_client', event => {
        if (event.message.command.toUpperCase() === 'CHATHISTORY') {
            return handleCommand(event);
        }
    });

    hooks.on('available_isupports', async event => {
        // Some clients actively use CHATHISTORY so offer both. CHATHISTORY clients will be
        // expected to updated their chathistry specification as it progresses through IRCv3
        event.tokens.push('CHATHISTORY=' + MAX_MESSAGES);
        event.tokens.push('DRAFT/CHATHISTORY=' + MAX_MESSAGES);
    });
};

async function handleCommand(event) {
    event.preventDefault();
    event.passthru = false;

    let msg = event.message;
    let con = event.client;
    let messageDb = con.messages;

    let subCmd = mParamU(msg, 0, '');
    let target = mParam(msg, 1, '');

    let messages = [];

    if (subCmd === 'TARGETS') {
        return commandTargets(msg, con, messageDb);
    } else if (subCmd === 'BEFORE' || subCmd === 'AFTER') {
        messages = await commandBeforeOrAfter(msg, con, messageDb);
    } else if (subCmd === 'LATEST') {
        messages = await commandLatest(msg, con, messageDb);
    } else if (subCmd === 'BETWEEN') {
        messages = await commandBetween(msg, con, messageDb);
    } else if (subCmd === 'AROUND') {
        messages = await commandAround(msg, con, messageDb);
    }

    if (messages) {
        let batchId = Math.round(Math.random()*1e17).toString(36);

        let m = new Irc.Message('BATCH', '+' + batchId, 'chathistory', target);
        m.prefix = 'bnc';
        con.writeMsg(m);

        messages.forEach(message => {
            message.tags.batch = batchId;
            con.writeMsg(message);
        });

        m = new Irc.Message('BATCH', '-' + batchId);
        m.prefix = 'bnc';
        con.writeMsg(m);
    }
};

async function commandTargets(msg, con, messageDb) {
    let fromRef = parseReference(mParam(msg, 1, ''));
    let toRef = parseReference(mParam(msg, 2, ''));
    let paramLimit = parseLimit(mParam(msg, 3, ''), MAX_MESSAGES);

    let networkId = con.state.authNetworkId;

    if (fromRef.type !== 'timestamp' || toRef.type !== 'timestamp') {
        con.writeMsg(
            'FAIL',
            'CHATHISTORY',
            'NEED_MORE_PARAMS',
            'TARGETS',
            'Timestamps should be given',
        );
        return;
    }

    let fromTime = dateToTs(fromRef.value);
    let toTime = dateToTs(toRef.value);

    let targets = await messageDb.getTargets(
        con.state.authUserId,
        networkId,
        Math.min(fromTime, toTime),
        Math.max(fromTime, toTime),
        paramLimit,
    );

    let batchId = Math.round(Math.random() * 1e17).toString(36);

    let m = new Irc.Message(
        'BATCH',
        '+' + batchId,
        'draft/chathistory-targets',
    );
    m.prefix = 'bnc';
    con.writeMsg(m);

    targets.forEach(target => {
        m = new Irc.Message(
            'CHATHISTORY',
            'TARGETS',
            target.buffer,
            new Date(target.time).toISOString(),
        );
        m.prefix = 'bnc';
        m.tags.batch = batchId;
        con.writeMsg(m);
    });

    m = new Irc.Message('BATCH', '-' + batchId);
    m.prefix = 'bnc';
    con.writeMsg(m);
}

async function commandAround(msg, con, messageDb) {
    // TODO: implement this
    con.writeMsg(
        'FAIL',
        'CHATHISTORY',
        'UNKNOWN_COMMAND',
        'AROUND',
        'Unknown command',
    );
}

async function commandBetween(msg, con, messageDb) {
    let messages = [];
    let target = mParam(msg, 1, '');
    let fromMsgRef = parseReference(mParam(msg, 2, ''));
    let toMsgRef = parseReference(mParam(msg, 3, ''));
    let paramLimit = parseLimit(mParam(msg, 4, ''), MAX_MESSAGES);

    let networkId = con.state.authNetworkId;

    if (
        (fromMsgRef.type === 'timestamp' || fromMsgRef.type === 'msgid') &&
        (toMsgRef.type === 'timestamp' || toMsgRef.type === 'msgid')
    ) {
        // messageDb requires timestanps in numeric format
        if (fromMsgRef.type === 'timestamp') {
            fromMsgRef.value = dateToTs(fromMsgRef.value);
        }
        if (toMsgRef.type === 'timestamp') {
            toMsgRef.value = dateToTs(toMsgRef.value);
        }
        messages = await messageDb.getMessagesBetween(
            con.state.authUserId,
            networkId,
            target,
            fromMsgRef,
            toMsgRef,
            paramLimit,
        );
    } else {
        con.writeMsg(
            'FAIL',
            'CHATHISTORY',
            'NEED_MORE_PARAMS',
            'BETWEEN',
            'Timestamp or message ID should be given',
        );
        return;
    }

    return messages;
}

async function commandLatest(msg, con, messageDb) {
    let messages = [];
    let target = mParam(msg, 1, '');
    let rawRef = mParam(msg, 2, '');
    let msgRef = parseReference(rawRef);
    let paramLimit = parseLimit(mParam(msg, 3, ''), MAX_MESSAGES);
    let networkId = con.state.authNetworkId;

    if (msgRef.type === 'timestamp') {
        if (rawRef === '*') {
            // '*' means no lower bound — return the last N messages
            messages = await messageDb.getMessagesBeforeTime(
                con.state.authUserId,
                networkId,
                target,
                Date.now(),
                paramLimit,
            );
        } else {
            // A specific timestamp was given — return messages AFTER that timestamp
            let ts = dateToTs(msgRef.value);
            messages = await messageDb.getMessagesFromTime(
                con.state.authUserId,
                networkId,
                target,
                ts,
                paramLimit,
            );
        }
    } else if (msgRef.type === 'msgid') {
        messages = await messageDb.getMessagesFromMsgId(
            con.state.authUserId,
            networkId,
            target,
            msgRef.value,
            paramLimit,
        );
    } else {
        con.writeMsg(
            'FAIL',
            'CHATHISTORY',
            'NEED_MORE_PARAMS',
            'LATEST',
            'Timestamp or message ID should be given',
        );
        return;
    }

    return messages;
}

async function commandBeforeOrAfter(msg, con, messageDb) {
    let messages = [];
    let subCmd = mParamU(msg, 0, '');
    let target = mParam(msg, 1, '');
    let msgRef = parseReference(mParam(msg, 2, ''));
    let paramLimit = parseLimit(mParam(msg, 3, ''), MAX_MESSAGES);
    let networkId = con.state.authNetworkId;

    if (msgRef.type === 'timestamp') {
        let ts = dateToTs(msgRef.value);

        if (subCmd === 'AFTER') {
            messages = await messageDb.getMessagesFromTime(
                con.state.authUserId,
                networkId,
                target,
                ts,
                paramLimit,
            );
        } else if (subCmd === 'BEFORE') {
            messages = await messageDb.getMessagesBeforeTime(
                con.state.authUserId,
                networkId,
                target,
                ts,
                paramLimit,
            );
        }
    } else if (msgRef.type === 'msgid') {
        // Pass the msgid through verbatim. It is matched against the stored
        // msgid column (a TEXT id), NOT a timestamp — running it through
        // dateToTs() would turn every id into NaN -> Date.now(), so the lookup
        // never matched and AFTER/BEFORE msgid returned zero messages. This is
        // the path the webchat gap-fill uses on reconnect (CHATHISTORY AFTER
        // msgid=<last held message>), so the bug left a hole in the backlog.
        let msgid = msgRef.value;

        if (subCmd === 'AFTER') {
            messages = await messageDb.getMessagesFromMsgId(
                con.state.authUserId,
                networkId,
                target,
                msgid,
                paramLimit,
            );
        } else if (subCmd === 'BEFORE') {
            messages = await messageDb.getMessagesBeforeMsgId(
                con.state.authUserId,
                networkId,
                target,
                msgid,
                paramLimit,
            );
        }
    } else {
        con.writeMsg(
            'FAIL',
            'CHATHISTORY',
            'NEED_MORE_PARAMS',
            subCmd,
            'Timestamp or message ID should be given',
        );
        return;
    }

    return messages;
}

// Convert a date string to a UTC time int, defaulting to now if it fails
function dateToTs(str) {
    let ts = new Date(str).getTime();
    if (isNaN(ts)) {
        ts = Date.now();
    }
    return ts;
}

// Convert 'timestamp=1234' or 'msgid=1234' into an object {type:msgid, value:1234}
function parseReference(str) {
    let ret = { type: '', value: '' };

    if (str === '*') {
        ret.type = 'timestamp';
        ret.value = Date.now();
    }

    let pos = str.indexOf('=');
    if (pos <= 0) {
        return ret;
    }

    ret.type = str.substr(0, pos).toLowerCase();
    ret.value = str.substr(pos + 1);
    return ret;
}

// Convert a string into an int within a maximum range
function parseLimit(str, max) {
    let limit = parseInt(str, 10);
    if (isNaN(limit) || limit > max || limit < 0) {
        limit = max;
    }
    return limit;
}