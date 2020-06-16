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

class Logger {
    log = (msg) => console.log(msg);
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

    const rabbitMQOptions = {
        //connUrl: 'amqp://guest:1237@localhost:5672',
        host,
        port,
        user,
        password,
        exchangeType: 'direct',
        exchange: 'direct-test',
        queue: 'test-queue'
    }

    console.log(
        `\nURl: ${rabbitMQOptions.connUrl}` +
        `\nexchange: ${rabbitMQOptions.exchange}` +
        `\nexchangeType: ${rabbitMQOptions.exchangeType}` +
        `\nconsumerQueue: ${rabbitMQOptions.queue}` +
        '\n'
    );

    let consumer = await Consumer.createConsumer(rabbitMQOptions,
        new Logger(),
        (thisConsumer, msg) => {
            console.log(`CONSUMER CALLBACK -> ${thisConsumer.id}, exchange: ${msg.fields.exchange}, ` +
                `routingKey: ${msg.fields.routingKey}, queue: ${thisConsumer.options.queue}, ` +
                `payload: ${JSON.stringify(Consumer.getJsonObject(msg))}`);

            if (count > maxCount)
                process.exit(0);
        }
     );

    if (!consumer.isReady()) {
        console.log('Error: consumer failure.');
        return;
    }

    let publisher = await Publisher.createPublisher(rabbitMQOptions, new Logger());

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
