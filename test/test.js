const _ = require('lodash');
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

const delay = (duration) =>
    new Promise(resolve =>
        setTimeout(() => {
            resolve();
        }, duration)
    );

(async function main() {
    const logger = new Logger();
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

    const consumer = await (await Consumer.createConsumer({
        connUrl,
        exchange,
        queue: consumerQueue,
        exchangeType,
        durable: true,
        noAck: true
    }))
    .startConsume((msg, jsonPayload, queue) => {
        logger.log(`consumer: ${consumer.id}, exchange: ${msg.fields.exchange}, ` +
            `routingKey: ${msg.fields.routingKey}, queue: ${queue}, ` +
            `message: ${JSON.stringify(jsonPayload)}`);
    });

    const publisher = await Publisher.createPublisher({
        connUrl,
        exchange,
        queue: '',
        exchangeType,
        durable: true,
        persistent: true
    });

    let count = 0;
    setInterval(async () => {
            publisher.publish(new Message(publisher.id, ++count, `text${count}`));
            await delay(1);
    }, 1000);
})();