const { Connection } = require('./connection');
const _ = require('lodash');
const utils = require('./utils');

module.exports.Consumer = class Consumer extends Connection {
    static createConsumer = async (options, externalLogger, fnConsume, isVerbose = true) =>
        await new Consumer(options, externalLogger, fnConsume, isVerbose).initialize();

    messages = [];
    chunkIntervalId;

    constructor(options, externalLogger, fnConsume, isVerbose = true) {
        super('consumer', options, externalLogger, isVerbose);

        this.fnConsume = _.isNil(fnConsume)
            ? (thisConsumer, msg) => this.messages = [...this.messages, msg]
            : fnConsume;

        if (this.isExchange && this.options.exchangeType === 'fanout')
            this.options.queue = `queue-${this.id}`;
    }

    async initialize() {
        await super.initialize();
        await this.startConsume();
        return this;
    }
    
    async startConsume() {
        try {
            if (this.isExchange)
                await this.channel.assertExchange(this.options.exchange, this.options.exchangeType, this.options);

            await this.channel.assertQueue(this.options.queue, { durable: this.options.durable });

            if (this.isExchange)
                await this.channel.bindQueue(this.options.queue, this.options.exchange, '');

            await this.channel.consume(this.options.queue,
                async msg => {
                    try {
                        await this.fnConsume(this, msg);
                    }
                    catch (err) {
                        this.logger.log(`Error in RabbitMQ consumer ${this.id}, in callback: ${err}`);
                    }
                },
                this.options);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ consumer ${this.id}, Consumer.startConsume(): ${err}`);
        }

        return this;
    }

    ack(...msgs) {
        if (!this.isReady() && utils.isEmpty(msgs))
            utils.flatten(msgs).forEach(msg => this.channel.ack(msg));
    }

    // ackAll() {
    //     if (!this.isReady())
    //         this.channel.ackAll();
    // }

    nack(...msgs) {
        if (!this.isReady() && utils.isEmpty(msgs))
            utils.flatten(msgs).forEach(msg => this.channel.nack(msg));
    }

    // nackAll() {
    //     if (!this.isReady())
    //         this.channel.nackAll();
    // }

    static getJsonObject = msg => JSON.parse(`${msg.content}`);

    static getPayloads = msg => utils.flatten(Consumer.getJsonObject(msg));

    static isRedelivered = msg => msg.fields.redelivered;

    startProcessChunks(fnProcessChunk, timeoutMs) {
        this.logger.log(`RabbitMQ consumer ${this.id}: startProcessChunks() called`);

        this.chunkIntervalId = setInterval(() => {
            if (this.messages.length === 0)
                return;

            let arrPayloads = [];
            let arrRedelivered = [];
            utils.flatten(this.messages).forEach(msg => {
                const payloads = Consumer.getPayloads(msg);
                const redelivered = Consumer.isRedelivered(msg);
                payloads.forEach(payload => {
                    arrPayloads = [...arrPayloads, payload];
                    arrRedelivered = [...arrRedelivered, redelivered];
                });
            });

            if (this.isVerbose)
                for (let i = 0; i < arrPayloads.length; i++)
                    this.logger.log(`RabbitMQ consumer ${this.id}: ` +
                        `message: ${JSON.stringify(arrPayloads[i])}, redelivered = ${arrRedelivered[i]}`);

            // Process payload
            let isOK = true;
            try {
                fnProcessChunk(arrPayloads);

                if (this.isVerbose)
                    this.logger.log(`RabbitMQ consumer ${this.id}: processing chunks called`);
            }
            catch (err) {
                isOK = false;
            }

            // ack / nack
            try {
                if (isOK) {
                    this.ack(this.messages);

                    if (this.isVerbose)
                        this.logger.log(`RabbitMQ consumer ${this.id}: Ack. ${this.messages.length} messages`);
                }
                else {
                    this.nack(this.messages);

                    if (this.isVerbose)
                        this.logger.log(`RabbitMQ consumer ${this.id}: Negative Ack. ${this.messages.length} messages`);
                }
            }
            catch (err) {
                this.logger.log(`Error in RabbitMQ consumer ${this.id}, ${isOK ? 'ack' : 'nack'}: ${err}`);
            }

            this.messages = [];
        },
        timeoutMs);

        return this;
    }

    stopProcessChunks() {
        this.logger.log(`RabbitMQ consumer ${this.id}: stopProcessChunks() called`);

        if (!_.isNil(this.chunkIntervalId)) {
            clearInterval(this.chunkIntervalId);
            this.chunkIntervalId = null;
        }
    }
}



