import { BigQuery } from '@google-cloud/bigquery';

export class BigQueryService {
  constructor() {
    this.datasetId = process.env.BIGQUERY_DATASET_ID;
    this.bigQueryClient = new BigQuery();
  }

  async insert(tableId, payload) {
    try {
      await this.bigQueryClient
        .dataset(this.datasetId)
        .table(tableId)
        .insert(payload);
    } catch (error) {
      console.error('Error import to BigQuery', JSON.stringify(error.errors));
    }
  }
}
