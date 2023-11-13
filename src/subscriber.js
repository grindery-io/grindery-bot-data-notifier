import { PubSub, Duration } from '@google-cloud/pubsub';
import { DataImporterService } from './services/data-importer-service.js';
import 'dotenv/config';

const pubSubClient = new PubSub();
const subscriptionName = process.env.PUBSUB_SUBSCRIPTION_NAME;

// Subscribe to messages from Pub/Sub
const subscriber = () => {
    const dataImporterService = new DataImporterService();
    // get subscription
    const subscription = pubSubClient.subscription(subscriptionName, {
      minAckDeadline: new Duration(
        parseInt(process.env.PUBSUB_MIN_ACK_DEADLINE, 10) || 60 * 1000
        ),
        maxAckDeadline: new Duration(
          parseInt(process.env.PUBSUB_MAX_ACK_DEADLINE, 10) || 1200 * 1000
          ),
          flowControl: {
            maxMessages: parseInt(process.env.PUBSUB_CONCURRENCY, 10) || 50,
          },
        });
        
        // Process and acknowledge pub/sub message
        const messageHandler = async (message) => {
          const messageDataString = message.data.toString();
          const messageData = JSON.parse(messageDataString);
          
      // console.log(
      //   `Received message [${message.id},${
      //     message.deliveryAttempt
      //   },${message.publishTime.toISOString()}]:`,
      //   JSON.stringify(messageData, null, 2)
      // );
      
      const deadline = Date.now() / 1000 - 60 * 60 * 24; // 1 day ago
      if (
        (message.deliveryAttempt || 0) > 2 &&
        message.publishTime.toStruct().seconds < deadline
      ) {
        console.log(
          `Dropping old message ${
            message.id
          }, publishTime=${message.publishTime.toISOString()}`
        );
        message.ack();
        return;
      }
  
      try {
        let processed = false;
        
        processed = await dataImporterService.importToBigQuery(messageData)
  
        if (!processed) {
          throw new Error('Error processing event');
        }
  
        // console.log(
        //   'Acknowledged message:',
        //   JSON.stringify(messageData, null, 2)
        // );

        setTimeout(() => message.ack(), 0);
       } catch (error) {
        console.error('messageHandler error:', error);
        setTimeout(() => message.nack(), 0); // "Nack" (don't acknowledge receipt of) the message
      }
    };

    subscription.on('message', messageHandler);
  };
  
  // Start listening for pub/sub messages
  subscriber();
  