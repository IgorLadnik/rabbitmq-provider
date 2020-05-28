const { CommonOptions, Connection } = require('./connection');
const { v4: uuidv4 } = require('uuid');
const _ = require('lodash');

module.exports.PublisherOptions = class PublisherOptions  extends CommonOptions {
    persistent;
}

module.exports.Publisher = class Publisher extends Connection {
    static createPublisher = async (po, fnLog) =>
        await new Publisher(po, fnLog).initialize();

    constructor(po, fnLog) {
        super(po, fnLog);
        this.id = `publisher-${uuidv4()}`;
    }

    async initialize() {
        await super.initialize();
        return this;
    }

    publish = (...arr) => {
        try {
            const strJson = Buffer.from(JSON.stringify(_.flatten(arr)));
            if (this.channel.publish(this.options.exchange, this.options.queue, strJson, this.options))
                this.logger.log(`RabbitMQ Publisher. Published: ${strJson}`);
        }
        catch (err) {
            this.logger.log(`Error in RabbitMQ, \"Publisher.publish()\": ${err}`);
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




