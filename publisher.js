const { Connection } = require('./connection');
const utils = require('./utils');

module.exports.Publisher = class Publisher extends Connection {
    static createPublisher = async (options, fnLog) =>
        await new Publisher(options, fnLog).initialize();

    constructor(options, fnLog) {
        super('publisher', options, fnLog);
        this.options.persistent = options.persistent || true;

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
            if (this.channel.publish(this.options.exchange, this.options.queue, strJson, this.options))
                this.logger.log(`RabbitMQ publisher \"${this.id}\" published: ${strJson}`);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ publisher \"${this.id}\", \"Publisher.publish()\": ${err}`);
        }
    }

    publishAsync = (...arr) =>
        new Promise(resolve =>
            setImmediate(() => {
                resolve();
                this.publish(...arr);
            })
        );

    // async purge() {
    //     try {
    //         await this.channel.purgeQueue(this.po.queue);
    //     }
    //     catch (err) {
    //         this.l.log(err);
    //     }
    // }
}




