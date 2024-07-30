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

        await channel.assertExchange(exchange, 'topic', { durable: true });
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

async function sendMessageToAPI(endpoint, body) {
    try {
        const config = {
            headers: {
                'Authorization': token,
                'Content-Type': 'application/json'
            }
        };
        console.log(body);
        const response = await axios.post(endpoint, body, config);
        console.log(`Response from API (${endpoint}):`, response.data.data);
        return response.data;
    } catch (error) {
        console.log(`Error sending to API (${endpoint}):`, error);
        return null;
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
        if(data.color == "NO DEFINIDO") {
            console.log("API no consumida");
        } else {
            await sendMessageToAPI(process.env.ENDPOINT_B, data);
        }
    } else if (data.tempYellow && data.humidityYellow && data.tempGreen && data.humidityGreen && data.peso) {
        console.log(data);
        const temperatureY = Number.parseFloat(data.tempYellow);
        const temperatureG = Number.parseFloat(data.tempGreen);
        const humidityY = Number.parseFloat(data.humidityYellow);
        const humidityG = Number.parseFloat(data.humidityGreen);
        const peso = Number.parseFloat(data.peso);

        const dataYellow = {
            "box": "Maduros",
            "temperature": temperatureY,
            "humidity": humidityY,
            "weight": 0
        };

        const dataGreen = {
            "box": "Verdes",
            "temperature": temperatureG,
            "humidity": humidityG,
            "weight": peso
        };
        const response = await sendMessageToAPI(process.env.ENDPOINT_M, dataYellow);
        if (response != null) {
            await sendMessageToAPI(process.env.ENDPOINT_M, dataGreen);
        } else {
            console.log("Algo salio mal");
        }
    } else {
        console.log("Message does not match any routing criteria:", data);
    }
};

const handleTokenMessage = async (data) => {
    if (data) {
        token = data;
        console.log("Token updated");
    } else {
        console.log("No token found in message");
    }
};

(async () => {
    await consumeQueue(mqttQueueConfig, handleMqttMessage);
    await consumeQueue(tokenQueueConfig, handleTokenMessage);
})();