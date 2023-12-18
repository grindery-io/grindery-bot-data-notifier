import { BigQuery } from '@google-cloud/bigquery';
import { Database } from '../db/conn.js';
import {
  DATA_SET_ID,
  STATUS_SUCCESS,
  SWAPS_COLLECTION,
  TABLE_ID_WALLET_USERS,
  TRANSFERS_COLLECTION,
  WALLET_USERS_COLLECTION,
} from '../utils/contants.js';
import axios from 'axios';
import 'dotenv/config';

const bigqueryClient = new BigQuery();
const db = await Database.getInstance();

export const mvuScore = async () => {
  const swapsCollection = db.collection(SWAPS_COLLECTION);
  const transfersCollection = db.collection(TRANSFERS_COLLECTION);
  const walletUsersCollection = db.collection(WALLET_USERS_COLLECTION);

  const startDate = new Date(new Date().toISOString());
  startDate.setUTCHours(startDate.getUTCHours() - 2); // Set to 2 hours ago

  const recentWallets = walletUsersCollection.find({
    webAppOpenedLastDate: { $gte: startDate },
  });

  while (await recentWallets.hasNext()) {
    try {
      const wallet = await recentWallets.next();

      const walletExistsInBigQuery = await checkWalletInBigQuery(
        wallet.userTelegramID,
      );

      if (walletExistsInBigQuery) {
        const swapsCount = await swapsCollection.countDocuments({
          userTelegramID: wallet.userTelegramID,
          status: STATUS_SUCCESS,
        });

        const transfersCount = await transfersCollection.countDocuments({
          senderTgId: wallet.userTelegramID,
          status: STATUS_SUCCESS,
        });

        const stakedAmount = (
          await axios.get(
            `https://wallet-api.grindery.io/v2/stake/${wallet.userTelegramID}`,
            {
              timeout: 100000,
              headers: {
                Authorization: `Bearer ${process.env.API_KEY}`,
                'Content-Type': 'application/json',
              },
            },
          )
        ).data?.amount;

        const walletFormatted = {
          userTelegramID: wallet.userTelegramID,
          swapsCount: swapsCount,
          transfersCount: transfersCount,
          stakedAmount: stakedAmount,
        };

        const query = `
            UPDATE ${DATA_SET_ID}.${TABLE_ID_WALLET_USERS}
            SET
            swapsCount = @swapsCount,
            transfersCount = @transfersCount,
            stakedAmount = @stakedAmount
            WHERE userTelegramID = @userTelegramID
        `;

        const options = {
          query: query,
          params: walletFormatted,
          types: {
            swapsCount: 'STRING',
            transfersCount: 'STRING',
            stakedAmount: 'STRING',
            userTelegramID: 'STRING',
          },
        };

        await bigqueryClient.query(options);
        console.log(
          `BIGQUERY - MVU Score user telegram id (${walletFormatted.userTelegramID}) updated in BigQuery`,
        );
      }
    } catch (error) {
      console.log(`BIGQUERY - Error processing mvu: ${error}`);
    }
  }
};

async function checkWalletInBigQuery(userTelegramID) {
  const query = `
      SELECT userTelegramID
      FROM ${DATA_SET_ID}.${TABLE_ID_WALLET_USERS}
      WHERE userTelegramID = '${userTelegramID}'
    `;
  const [rows] = await bigqueryClient.query(query);
  return rows.length > 0;
}
