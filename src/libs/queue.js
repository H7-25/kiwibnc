const EventEmitter = require('./eventemitter');
const ParallelQueue = require('./ParallelQueue');
const WaitGroup = require('./WaitGroup');
const Stats = require('./stats');
const amqp = require('amqplib/callback_api');
const { args } = require('commander');

module.exports = class Queue extends EventEmitter {
    constructor(conf) {
        super();
        this.host = conf.get('queue.amqp_host', 'amqp://localhost');
        this.queueToSockets = conf.get('queue.sockets_queue', 'q_sockets');
        this.queueToWorker = conf.get('queue.worker_queue', 'q_worker');
        this.channel = null;
        // The connection owning this.channel, kept so a reconnect can close it (see _closeConn)
        this.conn = null;
        this.consumerTag = '';
        this.closing = false;
        this.reconnecting = false;
        // Re-registers the consumer on a fresh channel after a reconnect.
        // Set by listenForEvents(); null on send-only instances.
        this._startConsuming = null;
        // The channel the consumer is currently bound to, so we re-arm exactly
        // once per channel (see ensureConsuming()).
        this._consumingOn = null;
        // Serialises concurrent connect() attempts (see connect()).
        this._connectPromise = null;
        this.closingWg = new WaitGroup();
        this.stats = Stats.instance().makePrefix('queue');
    }

    connect() {
        // Serialise concurrent connect attempts so callers racing to reconnect
        // (e.g. _send and scheduleReconnect after a drop) share a single channel
        // instead of each creating one and orphaning the consumer.
        if (this._connectPromise) {
            return this._connectPromise;
        }

        this._connectPromise = (async () => {
            try {
                await this._doConnect();
            } finally {
                this._connectPromise = null;
            }
        })();

        return this._connectPromise;
    }

    // Closes the connection we're about to replace and waits for the broker to release its
    // consumers. Never rejects - a connection being discarded has nothing useful to fail at.
    _closeConn() {
        let conn = this.conn;
        this.conn = null;
        this.channel = null;
        if (!conn) {
            return Promise.resolve();
        }

        return new Promise((resolve) => {
            let done = false;
            let finish = () => {
                if (done) {
                    return;
                }
                done = true;
                resolve();
            };

            // An already dead connection may never call back, and connect() shares one promise
            // between all its callers, so hanging here would block every later attempt
            setTimeout(finish, 2000).unref();
            try {
                conn.close(finish);
            } catch (err) {
                finish();
            }
        });
    }

    async _doConnect() {
        // A dead channel does not close its connection, and that connection still owns the
        // exclusive consumer on our queue. Opening a new one without closing it first gets
        // ACCESS_REFUSED, which closes the new channel and reconnects, forever.
        await this._closeConn();

        let {conn, channel} = await this.getChannel();
        await channel.assertQueue(this.queueToSockets, {durable: true});
        await channel.assertQueue(this.queueToWorker, {durable: true});
        await channel.prefetch(1000);

        // Channel error/close and connection error/close all mean this channel
        // is dead. Route every one of them through onLost so an unexpected drop
        // ALWAYS reconnects, while a manual stop (this.closing, set only by
        // stopListening()) never does.
        let onLost = (reason) => {
            // A late event from a connection we've already replaced must not
            // clobber the current channel or trigger a redundant reconnect.
            if (this.channel !== channel) {
                return;
            }
            this.channel = null;
            if (this.closing) {
                return;
            }
            l.warn(`AMQP ${reason}, scheduling reconnect`);
            this.scheduleReconnect();
        };

        // The conn 'error' handler is mandatory: without it an unexpected TCP
        // close emits an unhandled 'error' event and crashes the process.
        conn.on('error', (err) => l.warn('AMQP connection error:', err.message));
        conn.on('close', () => onLost('connection closed'));
        channel.on('error', (err) => l.warn('AMQP channel error:', err.message));
        channel.on('close', () => onLost('channel closed'));

        this.channel = channel;
        this.conn = conn;

        // Re-arm consumption on the fresh channel. No-op for send-only
        // instances and when already bound, so every reconnect path
        // (scheduleReconnect, _send, boot) restores the consumer exactly once.
        this.ensureConsuming();
    }

    // Binds the consumer to the current channel if it isn't already. Safe to
    // call after any (re)connect; only acts when there's a consumer to arm.
    ensureConsuming() {
        if (!this._startConsuming || !this.channel) {
            return;
        }
        if (this._consumingOn === this.channel) {
            return;
        }
        this._consumingOn = this.channel;
        this._startConsuming();
    }

    scheduleReconnect() {
        if (this.closing || this.reconnecting) {
            return;
        }
        this.reconnecting = true;

        let attempt = 0;
        let tryReconnect = async () => {
            if (this.closing) {
                this.reconnecting = false;
                return;
            }

            attempt++;
            try {
                // connect() re-arms the consumer via ensureConsuming().
                await this.connect();
                l.info(this._startConsuming
                    ? 'AMQP reconnected and consumer re-established'
                    : 'AMQP reconnected');
                this.reconnecting = false;
            } catch (err) {
                let delay = Math.min(30000, 1000 * Math.pow(2, attempt - 1));
                l.warn(`AMQP reconnect attempt ${attempt} failed: ${err.message}; retrying in ${delay}ms`);
                setTimeout(tryReconnect, delay);
            }
        };

        tryReconnect();
    }

    safeAck(msg, channel) {
        // A delivery tag only means anything on the channel that delivered it. Acking one
        // on a fresh channel is answered with PRECONDITION_FAILED, which closes that
        // channel and reconnects - and since every queued message from the old channel
        // does it in turn, the reconnects loop. Skipping is safe: the broker redelivers.
        if (channel && channel !== this.channel) {
            l.debug('AMQP message from a replaced channel, it will be redelivered');
            return;
        }
        // The channel can be nulled out mid-processing by an unexpected close
        // (the await in processMsgQueue yields). Skipping the ack is safe: the
        // broker redelivers unacked messages once the consumer is re-armed.
        if (!this.channel) {
            l.warn('AMQP channel unavailable, message will be redelivered');
            return;
        }
        try {
            this.channel.ack(msg);
        } catch (err) {
            l.warn('AMQP ack failed, message will be redelivered:', err.message);
        }
    }

    async initServer() {
        this.queueName = this.queueToSockets;
        await this.connect();
    }

    async initWorker() {
        this.queueName = this.queueToWorker;
        await this.connect();
    }

    async sendToWorker(type, data) {
        let payload = JSON.stringify([type, data]);
        l.trace('Queue sending to worker:', payload);
        this.stats.increment('sendtoworker');
        await this._send(this.queueToWorker, Buffer.from(payload), {persistent: true});
    }

    async sendToSockets(type, data) {
        let payload = JSON.stringify([type, data]);
        l.trace('Queue sending to sockets: ' + payload);
        this.stats.increment('sendtosockets');
        // Use non-persistent for connection.data to reduce disk I/O during channel dumps
        // Data can be regenerated if lost (client will reconnect)
        let persistent = (type !== 'connection.data');
        await this._send(this.queueToSockets, Buffer.from(payload), {persistent});
    }

    // Never rejects. Callers fire this from socket event handlers without awaiting, so a
    // rejection here is an unhandledRejection, which Node treats as fatal.
    async _send(queue, buffer, options) {
        try {
            if (!this.channel) {
                await this.connect();
            }
            this.channel.sendToQueue(queue, buffer, options);
            return;
        } catch (err) {
            // Broker gone, or the channel closed between the null check and the send
            this.channel = null;
            l.warn('AMQP sendToQueue failed, reconnecting and retrying:', err.message);
        }

        try {
            await this.connect();
            this.channel.sendToQueue(queue, buffer, options);
        } catch (retryErr) {
            l.error('AMQP sendToQueue failed after reconnect, dropping message:', retryErr.message);
            this.scheduleReconnect();
        }
    }

    async listenForEvents() {
        if (!this.channel) {
            await this.connect();
        }

        let queueName = this.queueName;
        this.closing = false;

        l.info('Listening on queue ' + queueName);
        let nextMsgId = 1;
        let q = new ParallelQueue();

        let cnt=0;
        let lastCnt=0;
        let inFlight = 0;
        let processMsgQueue = async () => {
            if (this.closing) {
                return;
            }

            if (inFlight >= 1000) {
                // Limit the number of messages we can process at once
                return;
            }

            let qMessage = q.get();
            if (!qMessage) {
                return;
            }

            this.closingWg.add('listenForEvents');
            let event = qMessage.item.event;
            let messageTmr = this.stats.timerStart('message.received.' + event[0]);

            inFlight++;
            try {
                await this.emit(event[0], event[1]);
            } catch (error) {
                l.error(error.stack);
            }
            inFlight--;

            messageTmr.stop();
            qMessage.ack();
            this.safeAck(qMessage.item.amqpMsg, qMessage.item.amqpChannel);

            cnt++;
            if (now() - lastCnt > 5) {
                let queues = (q.blocks && q.blocks[0] && q.blocks[0].queues) ? q.blocks[0].queues : null;
                let numQueues = queues ? Object.keys(queues).length : 0;
                l.debug(new Date(), 'Messages in 5sec:', cnt, 'inFlight:', inFlight, 'Num. queues:', numQueues);
                lastCnt = now();
                cnt = 0;
            }

            this.closingWg.done('listenForEvents');

            process.nextTick(() => {
                processMsgQueue();
            });
        };

        // Stored so scheduleReconnect() can re-register the consumer on a fresh
        // channel after a drop. Reuses the same queue/pipeline state above.
        this._startConsuming = () => {
            // Captured so each message is acked on the channel it arrived on, see safeAck()
            let channel = this.channel;
            channel.consume(queueName, (msg) => {
                if (this.closing) {
                    return;
                }

                // msg can be null in some cases such as a purged queue
                if (!msg) {
                    return;
                }

                // consumerTag is the same for every message here, but keeps tabs of it for future
                // use anyway.
                this.consumerTag = msg.fields.consumerTag;

                let id = 'msg' + ++nextMsgId;
                l.trace('Queue received:', id, msg.content.toString());
                let obj = JSON.parse(msg.content.toString());

                // Messages are expected to be an array of 2 items: [event_name, obj_of_params]
                if (!obj || obj.length !== 2) {
                    this.stats.increment('message.ignored');
                    this.safeAck(msg, channel);
                    return;
                }

                this.stats.increment('message.received');

                // Don't bother emitting if we have no events for it
                if (this.listenerCount(obj[0]) > 0) {
                    if (obj[1] && obj[1].id) {
                        // This event is related to a connection ID
                        let conId = obj[1].id;
                        q.add('connection', conId, {amqpMsg: msg, amqpChannel: channel, event: obj});
                    } else {
                        // An internal bnc event
                        q.add('bnc', 'internal', {amqpMsg: msg, amqpChannel: channel, event: obj});
                    }

                } else {
                    this.safeAck(msg, channel);
                }

                process.nextTick(() => {
                    processMsgQueue();
                });
            }, {noAck: false, exclusive: true});
        };

        this.ensureConsuming();
    }

    stopListening() {
        this.closing = true;

        return new Promise((resolve, reject) => {
            this.stats.increment('stopping');

            if (!this.consumerTag) {
                resolve();
                return;
            }

            this.closingWg.add('channel.cancel');
            if (!this.channel) {
                this.closingWg.done('channel.cancel');
                resolve();
                return;
            }
            try {
                this.channel.cancel(this.consumerTag, (err, ok) => {
                    this.closingWg.done('channel.cancel');
                    resolve();
                });
            } catch (err) {
                l.warn('AMQP channel cancel failed:', err.message);
                this.closingWg.done('channel.cancel');
                resolve();
            }
        })
        .then(() => this.closingWg.wait());
    }

    getChannel() {
        return new Promise((resolve, reject) => {
            this.stats.increment('connecting');
            let connectTmr = this.stats.timerStart('connecting.time');

            amqp.connect(this.host, (err, conn) => {
                connectTmr.stop();

                if (err) {
                    this.stats.increment('connecting.fail');
                    reject(err);
                    return;
                }

                this.stats.increment('connecting.success');
                this.stats.increment('connecting.time');
                conn.createChannel((err, channel) => {
                    if (err) {
                        // Nothing else references this connection, and leaving it open would
                        // hold the exclusive consumer against the next attempt
                        try { conn.close(); } catch (e) {}
                        reject(err);
                        return;
                    }

                    // Lifecycle handlers are attached in _doConnect, which owns
                    // both the conn and channel and the reconnect decision.
                    resolve({conn, channel});
                });
            });
        });
    }
}


function now() {
    return Math.floor(Date.now() / 1000);
}


