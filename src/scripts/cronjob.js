import cron from 'node-cron';
import {
  importUsersLast4Hours,
  importTransfersLast4Hours,
  importOrUpdateWalletUsersLast2Hours,
} from './bigquery.js';
import { mvuScore } from './mvu.js';

// Schedule a task to run every hour
cron.schedule('0 * * * *', async () => {
  console.log('CRON - importUsersLast4Hours task');
  try {
    await importUsersLast4Hours();
  } catch (error) {
    console.log('CRON - importUsersLast4Hours error ', error);
  }
});

// Schedule a task to run every hour
cron.schedule('0 * * * *', async () => {
  console.log('CRON - importTransfersLast4Hours task');
  try {
    await importTransfersLast4Hours();
  } catch (error) {
    console.log('CRON - importTransfersLast4Hours error ', error);
  }
});

// Schedule a task to run every hour
cron.schedule('0 * * * *', async () => {
  console.log('CRON - importOrUpdateWalletUsersLast2Hours task');
  try {
    await importOrUpdateWalletUsersLast2Hours();
  } catch (error) {
    console.log('CRON - importOrUpdateWalletUsersLast2Hours error ', error);
  }
});

// Schedule a task to run every hour
cron.schedule('0 * * * *', async () => {
  console.log('CRON - mvuScore task');
  try {
    await mvuScore();
  } catch (error) {
    console.log('CRON - mvuScore error ', error);
  }
});
