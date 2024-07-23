import amqp from 'amqplib';
import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

let token = '';

async function consumeQueue(queueConfig, handleMessage) {
    const { url, exchange, queueName } = queueConfig;

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
                const data = JSON.parse(messageContent);
                await handleMessage(data);
            }
        }, { noAck: true });
    } catch (error) {
        console.error(`Error in consumer for ${queueName}:`, error);
    }
}

async function sendMessageToAPI(endpoint, data) {
    try {
        const config = {
            headers: {
                Authorization: token,
            }
        };

        const response = await axios.post(endpoint, data, config);
        console.log(`Response from API (${endpoint}):`, response.data);
    } catch (error) {
        console.log(`Error sending to API (${endpoint}):`, error);
    }
}

const mqttQueueConfig = {
    url: process.env.URL,
    exchange: process.env.EXCHANGE,
    queueName: process.env.QUEUE,
};

const tokenQueueConfig = {
    url: process.env.URL,
    exchange: process.env.EXCHANGE_T,
    queueName: process.env.QUEUE_T,
};

const handleMqttMessage = async (data) => {
    if (data.color) {
        await sendMessageToAPI(process.env.ENDPOINT_B, data);
    } else if (data.box && data.temperature && data.humidity && data.weight) {
        await sendMessageToAPI(process.env.ENDPOINT_M, data);
    } else {
        console.log("Message does not match any routing criteria:", data);
    }
};

const handleTokenMessage = async (data) => {
    if (data) {
        token = data;
        console.log("Token updated:", token);
    } else {
        console.log("No token found in message");
    }
};

(async () => {
    await consumeQueue(mqttQueueConfig, handleMqttMessage);
    await consumeQueue(tokenQueueConfig, handleTokenMessage);
})();