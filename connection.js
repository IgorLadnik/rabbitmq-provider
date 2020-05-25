const { v4: uuidv4 } = require('uuid');
const amqp = require('amqplib');

module.exports.Connection = class Connection {
    connUrl;
    channel;
    l;

    constructor(connUrl, l) {
        this.connUrl = connUrl;
        this.l = l;
    }

    async connect() {
        try {
            return await amqp.connect(this.connUrl);
        }
        catch (err) {
            this.l.log(`Error in RabbitMQ Connection, \"Connection.connect()\", connUrl = \"${this.connUrl}\": ${err}`);
        }
    }

    async createChannelConnection() {
        let conn = await this.connect();
                   
        try {
            this.channel = await conn.createChannel();
        }
        catch (err) {
            this.l.log(`Error in RabbitMQ Connection, \"Connection.createChannelConnection()\": ${err}`);
        }
    }
}
