import amqp from 'amqplib';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

async function consumeQueue(queueConfig) {
    const { url, exchange, queueName, apiEndpoint } = queueConfig;

    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();

        await channel.assertExchange(exchange, 'topic', { durable: true });
        const queue = await channel.assertQueue(queueName, { exclusive: false });
        await channel.bindQueue(queue.queue, exchange, '');

        console.log(`Listening to events on ${queueName}...`);

        channel.consume(queue.queue, async (mensaje) => {
            if (mensaje !== null) {
                console.log(`Message received from ${queueName}: ${mensaje.content.toString()}`);

                try {
                    const data = JSON.parse(mensaje.content.toString());
                    if (data) {
                        const response = await axios.post(apiEndpoint, data);
                        console.log(`Response from API (${queueName}):`, response.data);
                    } else {
                        console.log("Data no content");
                    }
                } catch (error) {
                    console.log(`Error sending to API (${queueName}):`, error);
                }
            }
        }, { noAck: true });
    } catch (error) {
        console.error(`Error in consumer for ${queueName}:`, error);
    }
}

const queueConfigs = [
    {
        url: process.env.URL,
        exchange: process.env.EXCHANGE_B,
        queueName: process.env.QUEUE_B,
        apiEndpoint: process.env.ENDPOINT_B
    },
    {
        url: process.env.URL,
        exchange: process.env.EXCHANGE_M,
        queueName: process.env.QUEUE_M,
        apiEndpoint: process.env.ENDPOINT_M
    }
];

queueConfigs.forEach(config => consumeQueue(config));