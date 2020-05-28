const amqp = require('amqplib');
const _ = require('lodash');
const { v4: uuidv4 } = require('uuid');
const utils = require('./utils');

module.exports.CommonOptions = class CommonOptions {
    connUrl;
    exchange;
    queue;
    exchangeType;
    durable;

    id;
    retryIntervalMs;
    maxRetries;
}

module.exports.Connection = class Connection {
    constructor(objType, options, fnLog) {
        this.id = options.id || `${objType}-${uuidv4()}`;
        this.logger = new utils.Logger(fnLog);
        this.options = options;
        this.connUrl = options.connUrl;
        this.retryIntervalMs = options.retryIntervalMs || 5000;
        this.maxRetries = options.maxRetries || Number.MAX_SAFE_INTEGER;
        if (this.maxRetries < 1)
            this.maxRetries = Number.MAX_SAFE_INTEGER;
    }

    async initialize() {
        for (let i = 0; i < this.maxRetries; i++) {
            let conn;
            try {
                conn = await amqp.connect(this.connUrl);
            }
            catch (err) {
                this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", connUrl = \"${this.connUrl}\": ${err}`);
            }

            if (!_.isNil(conn)) {
                try {
                    this.channel = await conn.createChannel();
                }
                catch (err) {
                    this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\": ${err}`);
                }

                if (this.isReady()) {
                    conn.on('error', (err) => {
                        // Connection error handler
                        this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", \"${this.connUrl}\". ${err}. ` +
                                   `Connection problem.\n\nRECONNECTION by \"${this.id}\" to \"${this.connUrl}\"\n`);

                        // Async. reconnection
                        setImmediate(async () => await this.initialize());
                    });

                    return;
                }

                await utils.delayMs(this.retryIntervalMs);
            }
        }

        this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", \"${this.connUrl}\". ` +
                        'Failed to connect after max retries.');
    }

    stop() {
        if (this.isReady()) {
            this.channel.close();
            this.channel = null;
        }
    }

    isReady = () => !_.isNil(this.channel);
}
