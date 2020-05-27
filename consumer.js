const Connection = require('./connection').Connection;
const { v4: uuidv4 } = require('uuid');

module.exports.ConsumerOptions = class ConsumerOptions {
    connUrl;
    exchange;
    queue;
    exchangeType;
    durable;
    noAck;
}

module.exports.Consumer = class Consumer extends Connection {
    static createConsumer = async (co, l) =>
        await new Consumer(co, l).createChannel();

    constructor(co, l) {
        super(co.connUrl, l);
        this.id = `consumer-${uuidv4()}`;
        this.co = co;
        this.isExchange = co.exchange.length > 0 && co.exchangeType.length > 0;
        this.consumerQueue = co.queue;
    }

    async createChannel() {
        await super.createChannel();
        return this;
    }
    
    async startConsume(consumerFn) {
        try {
            if (this.isExchange)
                await this.channel.assertExchange(this.co.exchange, this.co.exchangeType, { durable: this.co.durable });

            await this.channel.assertQueue(this.co.queue, { durable: this.co.durable });

            if (this.isExchange)
                await this.channel.bindQueue(this.co.queue, this.co.exchange, '');

            await this.channel.consume(this.co.queue,
                (msg) => {
                    try {
                        consumerFn(msg, Consumer.getJsonObject(msg), this.consumerQueue);
                    }
                    catch (err) {
                        this.l.log(`Error in RabbitMQ Consumer, in consumer supplied callback: ${err}`);
                    }
                },
                { noAck: this.co.noAck });
        }
        catch (err) {
            this.l.log(`Error in RabbitMQ Consumer, \"Consumer.startConsume()\": ${err}`);
        }

        return this;
    }

    static getJsonObject = (msg) => JSON.parse(`${msg.content}`);
}



