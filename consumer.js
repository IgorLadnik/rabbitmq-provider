const { CommonOptions, Connection } = require('./connection');
const { v4: uuidv4 } = require('uuid');

module.exports.ConsumerOptions = class ConsumerOptions extends CommonOptions {
    noAck;
}

module.exports.Consumer = class Consumer extends Connection {
    static createConsumer = async (co, fnConsume, fnLog) =>
        await new Consumer(co, fnConsume, fnLog).initialize();

    constructor(co, fnConsume, fnLog) {
        super(co, fnLog);
        this.id = `consumer-${uuidv4()}`;
        this.isExchange = co.exchange.length > 0 && co.exchangeType.length > 0;
        this.consumerQueue = co.queue;
        this.fnConsume = fnConsume;
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

            await this.channel.assertQueue(this.consumerQueue, { durable: this.options.durable });

            if (this.isExchange)
                await this.channel.bindQueue(this.consumerQueue, this.options.exchange, '');

            await this.channel.consume(this.consumerQueue,
                (msg) => {
                    try {
                        this.fnConsume(msg, Consumer.getJsonObject(msg), this.consumerQueue);
                    }
                    catch (err) {
                        this.fnLog(`Error in RabbitMQ, in consumer supplied callback: ${err}`);
                    }
                },
                this.options);
        }
        catch (err) {
            this.fnLog(`Error in RabbitMQ, \"Consumer.startConsume()\": ${err}`);
        }

        return this;
    }

    static getJsonObject = (msg) => JSON.parse(`${msg.content}`);
}



