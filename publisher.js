const Connection = require('./connection').Connection;
const Logger = require('./logger').Logger;
const { v4: uuidv4 } = require('uuid');

module.exports.PublisherOptions = class PublisherOptions {
    connUrl;
    exchange;
    queue;
    exchangeType;
    durable;
    persistent;
}

module.exports.Publisher = class Publisher extends Connection {
    id;
    po;

    static createPublisher = async (po, l) =>
        await new Publisher(po, l || new Logger()).createChannel();

    constructor(po, l) {
        super(po.connUrl, l);
        this.id = `publisher-${uuidv4()}`;
        this.po = po;
    }

    async createChannel() {
        await this.createChannelConnection();
        return this;
    }

    publish = (...arr) => {
        const strJson = Buffer.from(JSON.stringify(arr));
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




