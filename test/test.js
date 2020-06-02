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

    const rabbitMQOptions = {
        connUrl: `amqp://${user}${password}${host}:${port}`, //'amqp://guest:1237@localhost:5672',
        exchange: 'direct-test',
        queue: 'test-queue',
        exchangeType: 'direct',
        retryIntervalMs: 5000,
        maxRetries: Number.MAX_SAFE_INTEGER
    }

    console.log(
        `\nURl: ${rabbitMQOptions.connUrl}` +
        `\nexchange: ${rabbitMQOptions.exchange}` +
        `\nexchangeType: ${rabbitMQOptions.exchangeType}` +
        `\nconsumerQueue: ${rabbitMQOptions.queue}` +
        '\n'
    );

    let consumer = await Consumer.createConsumer(rabbitMQOptions,
    (msg, jsonPayload, queue) => consumerCallback(msg, jsonPayload, queue, consumer.id),
     (msg) => console.log(msg)
     );

    if (!consumer.isReady()) {
        console.log('Error: consumer failure.');
        return;
    }

    let publisher = await Publisher.createPublisher(rabbitMQOptions,
    (msg) => console.log(msg)
    );

    if (!publisher.isReady()) {
        console.log('Error: publisher failure.');
        return;
    }

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

            publisher = null;
        }
    }, 1000);
})();