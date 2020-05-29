const _ = require('lodash');
const delayMs = require('../utils');
const Consumer = require('../consumer').Consumer;
const Publisher = require('../publisher').Publisher;
const prompt = require('prompt-sync')();

class Message {
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

const maxCount = Number.MAX_SAFE_INTEGER; //5;
let count = 0;

function consumerCallback(msg, jsonPayload, queue, consumerId) {
    console.log(`CONSUMER CALLBACK -> consumer: ${consumerId}, exchange: ${msg.fields.exchange}, ` +
               `routingKey: ${msg.fields.routingKey}, queue: ${queue}, ` +
               `message: ${JSON.stringify(jsonPayload)}`);

    if (count > maxCount)
        process.exit(0);
}

(async function main() {
    console.log('test app started');

    const retryIntervalMs = 5000;
    const maxRetries = Number.MAX_SAFE_INTEGER;

    // host = prompt('host? ');
    // port = prompt('port? ');
    // user = prompt('user? ');
    // password = prompt('password? ');

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

    let consumer = await Consumer.createConsumer({
        connUrl,
        exchange,
        queue: consumerQueue,
        exchangeType,
        durable: true,
        noAck: true,
        retryIntervalMs,
        maxRetries
    },
    (msg, jsonPayload, queue) => consumerCallback(msg, jsonPayload, queue, consumer.id),
     (msg) => console.log(msg)
     );

    if (!consumer.isReady()) {
        console.log('Error: consumer failure.');
        return;
    }

    let publisher = await Publisher.createPublisher({
        connUrl,
        exchange,
        queue: '',
        exchangeType,
        durable: true,
        persistent: true,
        retryIntervalMs,
        maxRetries
    },
    (msg) => console.log(msg)
    );

    if (!publisher.isReady()) {
        console.log('Error: publisher failure.');
        return;
    }

    console.log('1st SESSION');
    setInterval(async () => {
        if (!publisher.isReady())
            return;

        if (count < maxCount)
            publisher.publish(new Message(publisher.id, ++count, `text${count}`));
        else {
            publisher.stop();
            consumer.stop();

            count = maxCount;

            await delayMs(2000);

            console.log('2nd SESSION');
            await (await consumer.initialize())
                .startConsume((msg, jsonPayload, queue) => consumerCallback(msg, jsonPayload, queue, consumer.id));

            await publisher.initialize();
            await publisher.publishAsync(new Message(publisher.id, ++count, `text${count}`));

            publisher = null;
        }
    }, 1000);
})();