const amqp = require('amqplib');
const _ = require('lodash');
const { v4: uuidv4 } = require('uuid');
const { Logger } = require('./logger');
const utils = require('./utils');

module.exports.Connection = class Connection {
    constructor(objType, options, externalLogger) {
        this.id = options.id || `${objType}-${uuidv4()}`;
        this.logger = new Logger(externalLogger);

        this.options = { };

        if (_.isNil(options.connUrl)) {
            try {
                this.options.connUrl = `amqp://${options.user}${options.password}${options.host}:${options.port}`; //'amqp://guest:1237@localhost:5672',
            }
            catch (err) {
                this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.ctor()\", failed to create connUrl: ${err}`);
                return;
            }
        }

        this.options.connUrl = options.connUrl;
        this.options.exchange = options.exchange;
        this.options.queue = options.queue;
        this.options.exchangeType = options.exchangeType;
        this.options.durable = options.durable || true;
        this.options.noAck = options.noAck || false;
        this.options.persistent = options.persistent || true;

        this.retryIntervalMs = options.retryIntervalMs || 5000;
        this.maxRetries = options.maxRetries || Number.MAX_SAFE_INTEGER;

        if (this.maxRetries < 1)
            this.maxRetries = Number.MAX_SAFE_INTEGER;

        this.isExchange = !utils.isEmpty(this.options.exchange) && !utils.isEmpty(this.options.exchangeType);
        if (this.isExchange)
            this.options.exchangeType = this.options.exchangeType.toLowerCase();
    }

    async initialize() {
        for (let i = 0; i < this.maxRetries; i++) {
            let conn;
            try {
                conn = await amqp.connect(this.options.connUrl);
            }
            catch (err) {
                this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", connUrl = \"${this.options.connUrl}\": ${err}`);
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
                        this.logger.log(
                            `Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", \"${this.options.connUrl}\". ${err}. ` +
                            `Connection problem.\n\nRECONNECTION by \"${this.id}\" to \"${this.options.connUrl}\"\n`);

                        // Async. reconnection
                        setImmediate(async () => await this.initialize());
                    });

                    return;
                }

                await utils.delayMs(this.retryIntervalMs);
            }
        }

        this.logger.log(`Error in RabbitMQ \"${this.id}\", \"Connection.initialize()\", \"${this.options.connUrl}\". ` +
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
