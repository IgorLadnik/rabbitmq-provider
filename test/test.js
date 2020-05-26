const _ = require('lodash');
const Connection = require('../connection').Connection;
const Consumer = require('../consumer').Consumer;
const Publisher = require('../publisher').Publisher;
const Logger = require('../logger').Logger;
const prompt = require('prompt-sync')();

class Message {
    publisher;
    id;
    text;

    constructor(publisher, id, text) {
        this.publisher = publisher;
        this.id = id;
        this.text = text;
    }
}

let host;
let user;
let password;
let port;

const maxCount = 5;
let count = 0;

const logger = new Logger();

function consumerCallback(msg, jsonPayload, queue, consumerId) {
    logger.log(`consumer: ${consumerId}, exchange: ${msg.fields.exchange}, ` +
            `routingKey: ${msg.fields.routingKey}, queue: ${queue}, ` +
            `message: ${JSON.stringify(jsonPayload)}`);

    if (count > maxCount)
        process.exit(0);
}

(async function main() {
    logger.log('test app started');

    host = prompt('host? ');
    port = prompt('port? ');
    user = prompt('user? ');
    password = prompt('password? ');

    host = host || 'localhost';
    port = port || 5672;
    user = user || 'guest';
    password = password || '1237'

    if (!_.isNil(user) && user.length > 0)
        user += ':';

    if (!_.isNil(password) && password.length > 0)
        password += '@';

    const connUrl = `amqp://${user}${password}${host}:${port}`;

    const exchange = 'direct-test';
    const exchangeType = 'direct';
    const consumerQueue = 'test-queue';

    //'amqp://guest:1237@localhost:5672',

    console.log(
        `\nURl: ${connUrl}` +
        `\nexchange: ${exchange}` +
        `\nexchangeType: ${exchangeType}` +
        `\nconsumerQueue: ${consumerQueue}` +
        '\n'
    );

    let consumer = await (await Consumer.createConsumer({
        connUrl,
        exchange,
        queue: consumerQueue,
        exchangeType,
        durable: true,
        noAck: true
    }))
    .startConsume((msg, jsonPayload, queue) => consumerCallback(msg, jsonPayload, queue, consumer.id));

    if (!consumer.isReady()) {
        logger.log('Error: consumer failure.');
        return;
    }

    let publisher = await Publisher.createPublisher({
        connUrl,
        exchange,
        queue: '',
        exchangeType,
        durable: true,
        persistent: true
    });

    if (!publisher.isReady()) {
        logger.log('Error: publisher failure.');
        return;
    }

    logger.log('1st SESSION');
    setInterval(async () => {
        if (!publisher.isReady())
            return;

        if (count < maxCount)
            publisher.publish(new Message(publisher.id, ++count, `text${count}`));
        else {
            publisher.stop();
            consumer.stop();

            count = maxCount;

            await Connection.delay(2000);

            logger.log('2nd SESSION');
            await (await consumer.createChannel())
                .startConsume((msg, jsonPayload, queue) => consumerCallback(msg, jsonPayload, queue, consumer.id));

            await publisher.createChannel();
            await publisher.publishAsync(new Message(publisher.id, ++count, `text${count}`));

            publisher = null;
        }
    }, 1000);
})();