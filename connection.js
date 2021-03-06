const amqp = require('amqplib');
const _ = require('lodash');
const { v4: uuidv4 } = require('uuid');
const { Logger } = require('./logger');
const utils = require('./utils');

module.exports.Connection = class Connection {
    constructor(objType, options, externalLogger, isVerbose = true) {
        this.id = options.id || `${objType}-${uuidv4()}`;
        this.logger = new Logger(externalLogger);
        this.isVerbose = isVerbose;

        if (this.isVerbose)
            this.logger.log(`RabbitMQ ${this.id}: setting options: ${JSON.stringify(options)}`);

        this.options = { };

        this.options.connUrl = this.getConnUrl(options);
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

    getConnUrl(options) {
        let connUrl = options.connUrl;
        if (_.isNil(connUrl)) {
            try {
                let user = options.user;
                let password = options.password;
                if (!utils.isEmpty(user))
                    user += ':';

                if (!utils.isEmpty(password))
                    password += '@';

                connUrl = `amqp://${user}${password}${options.host}:${options.port}`;
            }
            catch (err) {
                connUrl = null;
                this.logger.log(`Error in RabbitMQ ${this.id}, Connection.ctor(), failed to create connUrl: ${err}`);
            }
        }

        return connUrl;
    }

    async initialize() {
        for (let i = 0; i < this.maxRetries; i++) {
            try {
                this.conn = await amqp.connect(this.options.connUrl);
            }
            catch (err) {
                this.logger.log(`Error in RabbitMQ ${this.id}, Connection.initialize(), connUrl = ${this.options.connUrl}: ${err}`);
            }

            if (!_.isNil(this.conn)) {
                try {
                    this.channel = await this.conn.createChannel();
                }
                catch (err) {
                    this.logger.log(`Error in RabbitMQ ${this.id}, Connection.initialize(): ${err}`);
                }

                if (this.isReady()) {
                    this.conn.on('error', (err) => {
                        // Connection error handler
                        this.logger.log(
                            `Error in RabbitMQ ${this.id}, Connection.initialize(), ${this.options.connUrl}. ${err}. ` +
                            `Connection problem.\n\nRECONNECTION by ${this.id} to ${this.options.connUrl}\n`);

                        // Async. reconnection
                        setImmediate(async () => await this.initialize());
                    });

                    return;
                }

                await utils.delayMs(this.retryIntervalMs);
            }
        }

        this.logger.log(`Error in RabbitMQ ${this.id}, Connection.initialize(), ${this.options.connUrl}. ` +
                        'Failed to connect after max retries.');
    }

    stop() {
        this.logger.log(`RabbitMQ ${this.id}: connection stop() called`);

        if (this.isReady()) {
            this.channel.close();
            this.conn.close();
            this.channel = null;
        }
    }

    isReady = () => !_.isNil(this.channel);
}
