import { Database } from '../db/conn.js';
import { BigQuery } from '@google-cloud/bigquery';
import web3 from 'web3';
import {
  DATA_SET_ID,
  STATUS_SUCCESS,
  SWAPS_COLLECTION,
  TABLE_ID_TRANSFERS,
  TABLE_ID_USERS,
  TABLE_ID_WALLET_USERS,
  TRANSFERS_COLLECTION,
  USERS_COLLECTION,
  WALLET_USERS_COLLECTION,
} from '../utils/contants.js';
import axios from 'axios';

const bigqueryClient = new BigQuery();
const datasetId = DATA_SET_ID;

const importUsers = async () => {
  const db = await Database.getInstance();
  const collection = db.collection(USERS_COLLECTION);

  // Get all users from the database
  const allUsers = await collection.find({}).toArray();

  if (allUsers.length === 0) {
    console.log('BIGQUERY - No users found in MongoDB.');
    process.exit(0);
  }

  const batchSize = 3000;
  let importedCount = 0;

  const existingPatchwallets = await getExistingPatchwallets(USERS_TABLE_ID);

  for (let i = 0; i < allUsers.length; i += batchSize) {
    console.log(
      `BIGQUERY - importedCount ${importedCount} i ${i} allUsers ${allUsers.length}`,
    );
    const batch = allUsers.slice(i, i + batchSize);

    const filteredBatch = batch.filter((user) => {
      return !existingPatchwallets.includes(
        web3.utils.toChecksumAddress(user.patchwallet),
      );
    });

    if (filteredBatch.length === 0) {
      console.log(
        'BIGQUERY - All users in the batch already exist in BigQuery.',
      );
      continue;
    }

    if (filteredBatch.length !== 0) {
      const bigQueryData = filteredBatch.map((user) => {
        return {
          context_ip: null,
          id: user._id.toString(),
          context_library_name: null,
          context_library_version: null,
          email: null,
          industry: null,
          loaded_at: null,
          name: user.userName,
          received_at: new Date(),
          uuid_ts: new Date(user.dateAdded),
          user_name: user.userName,
          user_telegram_id: user.userTelegramID,
          patchwallet: user.patchwallet,
          response_path: user.responsePath,
          user_handle: user.userHandle,
          attributes: JSON.stringify(user.attributes),
        };
      });

      await bigqueryClient
        .dataset(datasetId)
        .table(USERS_TABLE_ID)
        .insert(bigQueryData);
    }

    importedCount += filteredBatch.length;
  }

  process.exit(0);
};

const importTransfers = async () => {
  const db = await Database.getInstance();
  const collection = db.collection(TRANSFERS_COLLECTION);

  const allTransfers = await collection.find({}).toArray();

  if (allTransfers.length === 0) {
    console.log('BIGQUERY - No transfers found in MongoDB.');
    return;
  }

  const batchSize = 10000;
  let insertedCount = 0;

  const existingTransactionHashes = await getExistingTransactionHashes(
    TRANSFERS_TABLE_ID,
  );
  console.log('BIGQUERY - existingTransactionHashes');

  for (let i = 0; i < allTransfers.length; i += batchSize) {
    console.log(
      `BIGQUERY - insertedCount ${insertedCount} allTransfers ${allTransfers.length} i ${i}`,
    );
    const batch = allTransfers.slice(i, i + batchSize);

    const filteredBatch = batch.filter((transfer) => {
      return !existingTransactionHashes.includes(transfer.transactionHash);
    });

    if (filteredBatch.length === 0) {
      console.log(
        'BIGQUERY All transfers in the batch already exist in BigQuery.',
      );
      continue;
    }

    if (filteredBatch.length !== 0) {
      const bigQueryData = filteredBatch.map((transfer) => {
        return {
          amount: transfer.tokenAmount,
          context_library_name: null,
          context_library_version: null,
          event: null,
          event_text: null,
          id: transfer._id.toString(),
          loaded_at: null,
          original_timestamp: null,
          received_at: new Date(),
          sent_at: null,
          timestamp: new Date(transfer.dateAdded),
          user_id: null,
          uuid_ts: null,
          chain_id: transfer.chainId,
          recipient_tg_id: transfer.recipientTgId,
          recipient_wallet: transfer.recipientWallet,
          sender_name: transfer.senderName,
          sender_tg_id: transfer.senderTgId,
          sender_wallet: transfer.senderWallet,
          token_address: transfer.tokenAddress,
          token_amount: transfer.tokenAmount,
          token_symbol: transfer.tokenSymbol,
          transaction_hash: transfer.transactionHash,
          sender_handle: transfer.senderHandle,
          event_id: transfer.eventId,
        };
      });

      await bigqueryClient
        .dataset(datasetId)
        .table(TRANSFERS_TABLE_ID)
        .insert(bigQueryData);
    }

    insertedCount += filteredBatch.length;
  }

  process.exit(0);
};

export const importWalletUsers = async () => {
  const db = await Database.getInstance();
  const collection = db.collection(WALLET_USERS_COLLECTION);

  // Get all users from the database
  const allWalletUsers = await collection.find({}).toArray();

  if (allWalletUsers.length === 0) {
    console.log('BIGQUERY - No wallet users found in MongoDB.');
    process.exit(0);
  }

  const batchSize = 3000;
  let importedCount = 0;

  for (let i = 0; i < allWalletUsers.length; i += batchSize) {
    console.log(
      `BIGQUERY - importedCount ${importedCount} i ${i} allWalletUsers ${allWalletUsers.length}`,
    );
    const batch = allWalletUsers.slice(i, i + batchSize);

    if (batch.length !== 0) {
      const bigQueryData = batch.map((wallet) => {
        return {
          userTelegramID: wallet.userTelegramID ? wallet.userTelegramID : null,
          webAppOpened: wallet.webAppOpened ? wallet.webAppOpened : null,
          webAppOpenedFirstDate: wallet.webAppOpenedFirstDate
            ? wallet.webAppOpenedFirstDate
            : null,
          webAppOpenedLastDate: wallet.webAppOpenedLastDate
            ? wallet.webAppOpenedLastDate
            : null,
          telegramSessionSavedDate: wallet.telegramSessionSavedDate
            ? wallet.telegramSessionSavedDate
            : null,
          dateAdded: wallet.dateAdded ? wallet.dateAdded : null,
          balance: wallet.balance ? JSON.stringify(wallet.balance) : null,
          debug: wallet.debug ? JSON.stringify(wallet.debug) : null,
        };
      });

      await bigqueryClient
        .dataset(datasetId)
        .table(TABLE_ID_WALLET_USERS)
        .insert(bigQueryData);
    }

    importedCount += batch.length;
  }

  process.exit(0);
};

export const importUsersLast4Hours = async () => {
  const db = await Database.getInstance();
  const collection = db.collection(USERS_COLLECTION);

  const endDate = new Date();
  const startDate = new Date();
  startDate.setHours(startDate.getHours() - 4);

  const recentUsers = collection.find({
    dateAdded: { $gte: startDate, $lte: endDate },
  });
  const countRecentUsers = await recentUsers.count();

  const existingPatchwallets = await getExistingPatchwalletsLast24Hours(
    TABLE_ID_USERS,
    startDate,
    endDate,
  );

  let hasUsers = false;
  let index = 0;
  while (await recentUsers.hasNext()) {
    try {
      index++;
      hasUsers = true;

      console.log(
        `BIGQUERY - Total count recent users ${countRecentUsers} current index ${index}`,
      );

      const user = await recentUsers.next();

      const userExistsInBigQuery = existingPatchwallets.includes(
        web3.utils.toChecksumAddress(user.patchwallet),
      );

      if (userExistsInBigQuery) {
        console.log(
          `BIGQUERY - User wallet (${user.patchwallet}) already exists in BigQuery`,
        );
        continue;
      }

      const transformedUserData = {
        context_ip: null,
        id: user._id.toString(),
        context_library_name: null,
        context_library_version: null,
        email: null,
        industry: null,
        loaded_at: null,
        name: user.userName,
        received_at: new Date(),
        uuid_ts: new Date(user.dateAdded),
        user_name: user.userName,
        user_telegram_id: user.userTelegramID,
        patchwallet: user.patchwallet,
        response_path: user.responsePath,
        user_handle: user.userHandle,
        attributes: JSON.stringify(user.attributes),
      };

      await bigqueryClient
        .dataset(datasetId)
        .table(TABLE_ID_USERS)
        .insert(transformedUserData);

      console.log(
        `BIGQUERY - User wallet (${user.patchwallet}) added in BigQuery`,
      );
    } catch (error) {
      console.log(`BIGQUERY - Error importing to BigQuery ${error}`);
    }
  }

  if (!hasUsers) {
    console.log('BIGQUERY - No users found in MongoDB in the last 4 hours.');
  }

  console.log('BIGQUERY - Import completed successfully.');
};

export const importTransfersLast4Hours = async () => {
  const db = await Database.getInstance();
  const collection = db.collection(TRANSFERS_COLLECTION);

  // Calculate the date 24 hours ago
  const endDate = new Date();
  const startDate = new Date();
  startDate.setHours(startDate.getHours() - 4);

  // Find transfers in the last 24 hours using Cursor
  const allTransfers = collection.find({
    dateAdded: { $gte: startDate, $lte: endDate },
  });
  const countRecentTransfers = await allTransfers.count();

  const existingTransactionHashes =
    await getExistingTransactionHashesLast24Hours(
      TABLE_ID_TRANSFERS,
      startDate,
      endDate,
    );

  let hasTransfers = false;
  let index = 0;
  while (await allTransfers.hasNext()) {
    try {
      index++;
      hasTransfers = true;

      console.log(
        `BIGQUERY - Total count recent transfers ${countRecentTransfers} current index ${index}`,
      );

      const transfer = await allTransfers.next();

      const transferExistsInBigQuery = existingTransactionHashes.includes(
        transfer.transactionHash,
      );

      if (transferExistsInBigQuery) {
        console.log(
          `BIGQUERY - Transfer hash (${transfer.transactionHash}) already exists in BigQuery`,
        );
        continue;
      }

      const bigQueryData = {
        amount: transfer.tokenAmount,
        context_library_name: null,
        context_library_version: null,
        event_id: transfer.eventId,
        event_text: null,
        id: transfer._id.toString(),
        loaded_at: null,
        original_timestamp: null,
        received_at: new Date(),
        sent_at: null,
        timestamp: new Date(transfer.dateAdded),
        user_id: null,
        uuid_ts: null,
        chain_id: transfer.chainId,
        recipient_tg_id: transfer.recipientTgId,
        recipient_wallet: transfer.recipientWallet,
        sender_name: transfer.senderName,
        sender_tg_id: transfer.senderTgId,
        sender_wallet: transfer.senderWallet,
        token_address: transfer.tokenAddress,
        token_amount: transfer.tokenAmount,
        token_symbol: transfer.tokenSymbol,
        transaction_hash: transfer.transactionHash,
        sender_handle: transfer.senderHandle,
      };

      await bigqueryClient
        .dataset(datasetId)
        .table(TABLE_ID_TRANSFERS)
        .insert(bigQueryData);

      console.log(
        `BIGQUERY - transfer hash (${transfer.transactionHash}) added in BigQuery`,
      );
    } catch (error) {
      console.error('BIGQUERY - Error during import:', error);
    }
  }

  if (!hasTransfers) {
    console.log(
      'BIGQUERY - No transfers found in the last 4 hours in MongoDB.',
    );
  }

  console.log('BIGQUERY - Import completed successfully.');
};

export const importOrUpdateWalletUsersLast2Hours = async () => {
  const db = await Database.getInstance();
  const walletUsersCollection = db.collection(WALLET_USERS_COLLECTION);

  const startDate = new Date(new Date().toISOString());
  startDate.setUTCHours(startDate.getUTCHours() - 2); // Set to 2 hours ago

  const recentWallets = walletUsersCollection.find({
    webAppOpenedLastDate: { $gte: startDate },
  });

  console.log(
    `BIGQUERY - Wallet Users recent count (${await recentWallets.count()})`,
  );

  let hasWallets = false;
  while (await recentWallets.hasNext()) {
    try {
      hasWallets = true;
      const wallet = await recentWallets.next();

      const walletExistsInBigQuery = await checkWalletInBigQuery(
        wallet.userTelegramID,
      );

      const swapsCount = await db.collection(SWAPS_COLLECTION).countDocuments({
        userTelegramID: wallet.userTelegramID,
        status: STATUS_SUCCESS,
      });

      const transfersCount = await db
        .collection(TRANSFERS_COLLECTION)
        .countDocuments({
          senderTgId: wallet.userTelegramID,
          status: STATUS_SUCCESS,
        });

      let stakedAmount = 0.0;

      try {
        stakedAmount = (
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
      } catch (error) {
        console.log(`BIGQUERY - Error getting stake amount ${error})`);
        stakedAmount = 0.0;
      }

      const walletFormatted = {
        userTelegramID: wallet.userTelegramID ? wallet.userTelegramID : null,
        webAppOpened: wallet.webAppOpened ? wallet.webAppOpened : null,
        webAppOpenedFirstDate: wallet.webAppOpenedFirstDate
          ? wallet.webAppOpenedFirstDate
          : null,
        webAppOpenedLastDate: wallet.webAppOpenedLastDate
          ? wallet.webAppOpenedLastDate
          : null,
        telegramSessionSavedDate: wallet.telegramSessionSavedDate
          ? wallet.telegramSessionSavedDate
          : null,
        dateAdded: wallet.dateAdded ? wallet.dateAdded : null,
        balance: wallet.balance ? JSON.stringify(wallet.balance) : null,
        debug: wallet.debug ? JSON.stringify(wallet.debug) : null,
        swapsCount: swapsCount ? swapsCount : 0,
        transfersCount: transfersCount ? transfersCount : 0,
        stakedAmount: stakedAmount ? stakedAmount : 0.0,
      };

      if (walletExistsInBigQuery) {
        console.log(
          `BIGQUERY - Wallet User Telegram ID (${walletFormatted.userTelegramID}) exist`,
        );

        const query = `
          UPDATE ${datasetId}.${TABLE_ID_WALLET_USERS}
          SET
            webAppOpened = @webAppOpened,
            webAppOpenedFirstDate = @webAppOpenedFirstDate,
            webAppOpenedLastDate = @webAppOpenedLastDate,
            telegramSessionSavedDate = @telegramSessionSavedDate,
            dateAdded = @dateAdded,
            balance = @balance,
            debug = @debug,
            swapsCount = @swapsCount,
            transfersCount = @transfersCount,
            stakedAmount = @stakedAmount
          WHERE userTelegramID = @userTelegramID
        `;

        const options = {
          query: query,
          params: walletFormatted,
          types: {
            webAppOpened: 'STRING',
            webAppOpenedFirstDate: 'STRING',
            webAppOpenedLastDate: 'STRING',
            telegramSessionSavedDate: 'STRING',
            dateAdded: 'STRING',
            balance: 'STRING',
            debug: 'STRING',
            userTelegramID: 'STRING',
            swapsCount: 'STRING',
            transfersCount: 'STRING',
            stakedAmount: 'STRING',
          },
        };

        await bigqueryClient.query(options);
        console.log(
          `BIGQUERY - Wallet User Telegram ID (${walletFormatted.userTelegramID}) updated in BigQuery`,
        );
      } else {
        console.log(
          `BIGQUERY - Wallet User Telegram ID (${walletFormatted.userTelegramID}) does not exist`,
        );

        await bigqueryClient
          .dataset(datasetId)
          .table(TABLE_ID_WALLET_USERS)
          .insert(walletFormatted);

        console.log(
          `BIGQUERY - Wallet User Telegram ID (${walletFormatted.userTelegramID}) added in BigQuery`,
        );
      }
    } catch (error) {
      console.log(`BIGQUERY - Error processing wallet: ${error}`);
    }
  }

  if (!hasWallets) {
    console.log('BIGQUERY - No wallet users found in MongoDB.');
  }

  console.log('BIGQUERY - Wallet import/update process completed.');
};

async function getExistingPatchwallets(tableId) {
  const query = `SELECT patchwallet FROM ${datasetId}.${tableId}`;
  const [rows] = await bigqueryClient.query(query);

  return rows.map((row) => web3.utils.toChecksumAddress(row.patchwallet));
}

async function getExistingPatchwalletsLast24Hours(tableId, startDate, endDate) {
  const formattedStartDate = startDate.toISOString();
  const formattedEndDate = endDate.toISOString();

  const query = `
    SELECT patchwallet
    FROM ${datasetId}.${tableId}
    WHERE received_at >= '${formattedStartDate}'
    AND received_at <= '${formattedEndDate}'
  `;

  const [rows] = await bigqueryClient.query(query);

  return rows.map((row) => web3.utils.toChecksumAddress(row.patchwallet));
}

async function getExistingTransactionHashes(tableId) {
  const query = `SELECT transaction_hash FROM ${datasetId}.${tableId}`;
  const [rows] = await bigqueryClient.query(query);

  return rows.map((row) => row.transaction_hash);
}

async function getExistingTransactionHashesLast24Hours(
  tableId,
  startDate,
  endDate,
) {
  const formattedStartDate = startDate.toISOString();
  const formattedEndDate = endDate.toISOString();

  const query = `
    SELECT transaction_hash
    FROM ${datasetId}.${tableId}
    WHERE received_at >= '${formattedStartDate}'
    AND received_at <= '${formattedEndDate}'
  `;

  const [rows] = await bigqueryClient.query(query);

  return rows.map((row) => row.transaction_hash);
}

async function checkWalletInBigQuery(userTelegramID) {
  const query = `
    SELECT userTelegramID
    FROM ${datasetId}.${TABLE_ID_WALLET_USERS}
    WHERE userTelegramID = '${userTelegramID}'
  `;
  const [rows] = await bigqueryClient.query(query);
  return rows.length > 0;
}

importOrUpdateWalletUsersLast2Hours();
