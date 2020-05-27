const Connection = require('./connection').Connection;
const { v4: uuidv4 } = require('uuid');
const _ = require('lodash');

module.exports.PublisherOptions = class PublisherOptions {
    connUrl;
    exchange;
    queue;
    exchangeType;
    durable;
    persistent;
}

module.exports.Publisher = class Publisher extends Connection {
    static createPublisher = async (po, l) =>
        await new Publisher(po, ).createChannel();

    constructor(po, l) {
        super(po.connUrl, l);
        this.id = `publisher-${uuidv4()}`;
        this.po = po;
    }

    async createChannel() {
        await super.createChannel();
        return this;
    }

    publish = (...arr) => {
        const strJson = Buffer.from(JSON.stringify(_.flatten(arr)));
        if (this.channel.publish(this.po.exchange, this.po.queue, strJson /*, options*/))
            this.l.log(strJson);
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




