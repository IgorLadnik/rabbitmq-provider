const { Connection } = require('./connection');
const utils = require('./utils');

module.exports.Publisher = class Publisher extends Connection {
    static createPublisher = async (options, externalLogger, isVerbose = true) =>
        await new Publisher(options, externalLogger, isVerbose).initialize();

    constructor(options, externalLogger, isVerbose = true) {
        super('publisher', options, externalLogger, isVerbose);

        if (this.isExchange)
            this.options.queue = '';
    }

    async initialize() {
        await super.initialize();
        return this;
    }

    publish = (...arr) => {
        try {
            const strJson = Buffer.from(JSON.stringify(utils.flatten(arr)));
            if (this.channel.publish(this.options.exchange, this.options.queue, strJson, this.options) &&
                this.isVerbose)
                this.logger.log(`RabbitMQ publisher ${this.id} published: ${strJson}`);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ publisher ${this.id}, Publisher.publish(): ${err}`);
        }
    }

    publishAsync = (...arr) =>
        new Promise(resolve =>
            setImmediate(() => {
                resolve();
                this.publish(...arr);
            })
        );

    async purge() {
        this.logger.log(`RabbitMQ publisher ${this.id}: purge() called`);

        try {
            await this.channel.purgeQueue(this.po.queue);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ publisher ${this.id}, Publisher.purge(): ${err}`);
        }
    }
}




