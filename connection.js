const { v4: uuidv4 } = require('uuid');
const amqp = require('amqplib');
const _ = require('lodash');
const Logger = require('./logger').Logger;

module.exports.Connection = class Connection {
    retryInterval = 1000;
    maxRetries = 3;

    connUrl;
    channel;
    l;

    constructor(connUrl, l) {
        this.connUrl = connUrl;
        this.l = l || new Logger();
    }

    async createChannel() {
        for (let i = 0; i < this.maxRetries; i++) {
            let conn;
            try {
                conn = await amqp.connect(this.connUrl);
            }
            catch (err) {
                this.l.log(`Error in RabbitMQ, \"Connection.createChannel()\", connUrl = \"${this.connUrl}\": ${err}`);
            }

            if (_.isNil(conn))
                continue;

            try {
                this.channel = await conn.createChannel();
            }
            catch (err) {
                this.l.log(`Error in RabbitMQ, \"Connection.createChannel()\": ${err}`);
            }

            if (this.isReady())
                return;
            else
                await Connection.delay(this.retryInterval);
        }

        if (!this.isReady())
            this.l.log(`Error in RabbitMQ, \"Connection.createChannel()\": failed to connect after max retries.`);
    }

    stop() {
        if (this.isReady()) {
            this.channel.close();
            this.channel = null;
        }
    }

    isReady = () => !_.isNil(this.channel);

    static delay = (duration) =>
        new Promise(resolve =>
            setTimeout(() => {
                resolve();
            }, duration)
        );
}
