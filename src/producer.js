import { PubSub } from '@google-cloud/pubsub';
import 'dotenv/config';

const pubSubClient = new PubSub();
const topicName = process.env.PUBSUB_TOPIC_NAME;

const messageExample = {
  label1: "label1",
  label2: "label2"
}

const messageExample2 = {
  label3: "label3",
  label4: "label4"
}

const producer = async (message) => {
  try {
    const messageBuffer = Buffer.from(JSON.stringify(message));
    const messageId = await pubSubClient
      .topic(topicName)
      .publishMessage({ data: messageBuffer });

    console.log(`Message ${JSON.stringify(messageExample)} published - id ${messageId}`)
  } catch(e) {
    console.error(`Error while publishing message ${e.message}`)
  }
};
  
producer(messageExample);
producer(messageExample2);