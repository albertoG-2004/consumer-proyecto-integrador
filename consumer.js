import amqp from 'amqplib';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

let token = '';

async function consumeQueue(queueConfig) {
    const { url, exchange, queueName, apiEndpoint, isTokenQueue } = queueConfig;

    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();

        await channel.assertExchange(exchange, 'direct', { durable: true });
        const queue = await channel.assertQueue(queueName, { exclusive: false });
        await channel.bindQueue(queue.queue, exchange, '');

        console.log(`Listening to events on ${queueName}...`);

        channel.consume(queue.queue, async (mensaje) => {
            if (mensaje !== null) {
                const messageContent = mensaje.content.toString();
                // console.log(`Message received from ${queueName}: ${messageContent}`);
                const data = JSON.parse(messageContent);

                if (isTokenQueue) {
                    if (data) {
                        token = data;
                        console.log("Token updated");
                    } else {
                        console.log("No token found in message");
                    }
                } else {
                    try {
                        const config = {
                            headers: {
                                Authorization: token,
                            }
                        };

                        const response = await axios.post(apiEndpoint, data, config);
                        console.log(`Response from API (${queueName}):`, response.data);
                    } catch (error) {
                        console.log(`Error sending to API (${queueName}):`, error);
                    }
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
        apiEndpoint: process.env.ENDPOINT_B,
        isTokenQueue: false
    },
    {
        url: process.env.URL,
        exchange: process.env.EXCHANGE_M,
        queueName: process.env.QUEUE_M,
        apiEndpoint: process.env.ENDPOINT_M,
        isTokenQueue: false
    },
    {
        url: process.env.URL,
        exchange: process.env.EXCHANGE_T,
        queueName: process.env.QUEUE_T,
        isTokenQueue: true
    }
];

queueConfigs.forEach(config => consumeQueue(config));