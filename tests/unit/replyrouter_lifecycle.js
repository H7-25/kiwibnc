const { ircLineParser } = require('irc-framework');

describe('replyrouter request lifecycle through the real command handlers', () => {
    let hooks, upstream, client, dictionary;

    beforeEach(() => {
        jest.resetModules();
        global.l = Object.assign(jest.fn(), { debug: jest.fn(), info: jest.fn(), warn: jest.fn(),
            error: jest.fn(), trace: jest.fn() });
        global.config = { get: (key, fallback) => fallback };
        hooks = require('../../src/worker/hooks');
        require('../../src/extensions/replyrouter').init(hooks);
        const Outgoing = require('../../src/worker/connectionoutgoing');
        const Incoming = require('../../src/worker/connectionincoming');
        dictionary = new Map();
        const queue = { sendToSockets: jest.fn() };
        upstream = new Outgoing('upstream', {}, {}, queue, dictionary);
        upstream.state.loaded = true;
        upstream.state.connected = upstream.state.netRegistered = upstream.state.receivedMotd = true;
        upstream.state.nick = 'me';
        upstream.state.isupports = ['PREFIX=(ov)@+'];
        upstream.state.markDirty = jest.fn();
        const buffer = upstream.state.addBuffer('#test', upstream);
        buffer.addUser('Bob', { prefixes: ['@'] });
        client = new Incoming('client', {}, {}, {}, queue, dictionary);
        client.state.loaded = client.state.netRegistered = true;
        client.state.authUserId = client.state.authNetworkId = 1;
        client.state.nick = 'me';
        client.state.markDirty = jest.fn();
        client.state.destroy = jest.fn().mockResolvedValue();
        client.cachedUpstreamId = upstream.id;
        upstream.state.linkIncomingConnection(client.id);
        client.write = jest.fn();
    });

    afterEach(() => {
        delete global.l;
        delete global.config;
        jest.resetModules();
    });

    const request = (client, line) => client.messageFromClient(ircLineParser(line), line);
    const reply = (upstream, line) => upstream.messageFromUpstream(ircLineParser(line), line);
    const pending = client => client.state.tempGet('expecting_replies') || [];

    test('repeated local NAMES returns 353/366 without retaining completed requests', async () => {
        for (let n = 0; n < 100; n++) await request(client, 'NAMES #test');
        expect(client.write.mock.calls.filter(([line]) => line.includes(' 353 '))).toHaveLength(100);
        expect(client.write.mock.calls.filter(([line]) => line.includes(' 366 '))).toHaveLength(100);
        expect(pending(client)).toHaveLength(0);
    });

    test('unknown buffers and disconnected upstreams still receive local 366 without pending state', async () => {
        await request(client, 'NAMES #missing');
        upstream.state.connected = upstream.state.netRegistered = false;
        await request(client, 'NAMES #test');
        expect(client.write.mock.calls.filter(([line]) => line.includes(' 366 '))).toHaveLength(2);
        expect(pending(client)).toHaveLength(0);
    });

    test('bare NAMES completes when core consumes upstream 366, leaving WHOIS untouched', async () => {
        await request(client, 'NAMES');
        await request(client, 'WHOIS Bob');
        expect(pending(client).map(route => route.command)).toEqual(['NAMES', 'WHOIS']);
        await reply(upstream, ':server 353 me = #test :@Bob');
        expect(pending(client)).toHaveLength(2);
        await reply(upstream, ':server 366 me #test :End of /NAMES list.');
        expect(pending(client).map(route => route.command)).toEqual(['WHOIS']);
        expect(client.write.mock.calls.some(([line]) => line.includes(' 353 '))).toBe(true);
        expect(client.write.mock.calls.some(([line]) => line.includes(' 366 '))).toBe(true);
    });

    test('local NAMES does not consume concurrent upstream NAMES or WHOIS requests', async () => {
        await request(client, 'NAMES');
        await request(client, 'WHOIS Bob');
        await request(client, 'NAMES #test');
        expect(pending(client).map(route => route.command)).toEqual(['NAMES', 'WHOIS']);
    });

    test.each([
        ['WHOIS Bob', ':server 318 me Bob :End of WHOIS'],
        ['WHOWAS Bob', ':server 369 me Bob :End of WHOWAS'],
        ['LIST', ':server 323 me :End of LIST'],
        ['MOTD', ':server 376 me :End of MOTD'],
        ['MODE #test +b', ':server 368 me #test :End of ban list'],
        ['MODE #test +I', ':server 347 me #test :End of invite list'],
        ['MODE #test +e', ':server 349 me #test :End of exception list'],
        ['NAMES', ':server 401 me Bob :No such nick'],
    ])('%s completes through its normal ending numeric', async (command, ending) => {
        await request(client, command);
        expect(pending(client)).toHaveLength(1);
        await reply(upstream, ending);
        expect(pending(client)).toHaveLength(0);
        expect(client.write).toHaveBeenCalled();
    });

    test('concurrent matching routes do not duplicate a client in the delivery list', async () => {
        await request(client, 'WHOIS Bob');
        await request(client, 'WHOIS Alice');
        const result = await hooks.emit('message_to_clients', {
            clients: [client], message: ircLineParser(':server 311 me Bob user host * :Bob'),
        });
        expect(result.event.clients).toEqual([client]);
        expect(pending(client)).toHaveLength(2);
    });

    test('requests that cannot be sent upstream do not accumulate', async () => {
        upstream.state.connected = false;
        for (let n = 0; n < 20; n++) await request(client, 'WHOIS Bob');
        expect(pending(client)).toHaveLength(0);
    });

    test('upstream close cancels only that upstreams pending requests', async () => {
        await request(client, 'WHOIS Bob');
        const other = { state: { tempGet: jest.fn().mockReturnValue([{ command: 'LIST' }]), tempSet: jest.fn() } };
        dictionary.set('unrelated-client', other);
        await hooks.emit('connection_close', { upstream });
        expect(pending(client)).toHaveLength(0);
        expect(dictionary.get(client.id)).toBe(client);
        expect(other.state.tempSet).not.toHaveBeenCalled();
    });

    test('simultaneous requests from different clients keep one delivery per recipient', async () => {
        const Incoming = require('../../src/worker/connectionincoming');
        const second = new Incoming('second', {}, {}, {}, upstream.queue, dictionary);
        second.state.loaded = second.state.netRegistered = true;
        second.state.authUserId = second.state.authNetworkId = 1;
        second.state.nick = 'me';
        second.state.markDirty = jest.fn();
        second.cachedUpstreamId = upstream.id;
        second.write = jest.fn();
        upstream.state.linkIncomingConnection(second.id);
        await request(client, 'WHOIS Bob');
        await request(client, 'WHOIS Alice');
        await request(second, 'WHOIS Bob');
        await request(second, 'NAMES');
        await request(client, 'NAMES #test');
        const recipients = await hooks.emit('message_to_clients', {
            clients: [client, second], message: ircLineParser(':server 311 me Bob user host * :Bob'),
        });
        expect(recipients.event.clients).toEqual([client, second]);
        await reply(upstream, ':server 366 me #test :End of /NAMES list.');
        expect(pending(client).map(route => route.command)).toEqual(['WHOIS', 'WHOIS']);
        expect(pending(second).map(route => route.command)).toEqual(['WHOIS']);
    });

    test('client disconnect still destroys and unlinks the connection', async () => {
        await request(client, 'WHOIS Bob');
        await client.onClientClosed();
        expect(dictionary.has(client.id)).toBe(false);
        expect(upstream.state.linkedIncomingConIds.has(client.id)).toBe(false);
        expect(client.state.destroy).toHaveBeenCalledTimes(1);
    });
});
