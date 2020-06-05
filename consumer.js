const { Connection } = require('./connection');

module.exports.Consumer = class Consumer extends Connection {
    static createConsumer = async (options, fnConsume, fnLog) =>
        await new Consumer(options, fnConsume, fnLog).initialize();

    constructor(options, fnConsume, fnLog) {
        super('consumer', options, fnLog);
        this.fnConsume = fnConsume;
        this.options.noAck = options.noAck || true;

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
                async (msg) => {
                    try {
                        await this.fnConsume(msg, Consumer.getJsonObject(msg), this.options.queue);
                    }
                    catch (err) {
                        this.logger.log(`Error in RabbitMQ consumer \"${this.id}\", in callback: ${err}`);
                    }
                },
                this.options);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ consumer \"${this.id}\", \"Consumer.startConsume()\": ${err}`);
        }

        return this;
    }

    static getJsonObject = (msg) => JSON.parse(`${msg.content}`);
}



