import { BigQuery } from '@google-cloud/bigquery';
import 'dotenv/config';

const datasetId = process.env.BIGQUERY_DATASET_ID;
const tableId = process.env.BIGQUERY_DATASET_TABLE_ID;

export const addToBigQuery = async (messageData) => {
    // await bigqueryClient
    //     .dataset(datasetId)
    //     .table(tableId)
    //     .insert(messageData);

    return true;
}