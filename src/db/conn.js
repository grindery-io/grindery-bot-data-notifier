import { MongoClient } from 'mongodb';
import 'dotenv/config';

export class Database {
  /** Represents the instance of the database. */
  static instance;

  /**
   * Retrieves or creates an instance of the database.
   * If in production, connects to the actual MongoDB Atlas instance,
   * else utilizes a temporary in-memory MongoDB server.
   */
  static async getInstance() {
    // Check if an instance already exists
    if (!Database.instance) {
      // Create a MongoDB client using the Atlas URI
      const client = new MongoClient(process.env.ATLAS_URI);
      let conn;

      try {
        // Connect to the MongoDB Atlas instance
        conn = await client.connect();
      } catch (e) {
        console.error(e);
      }
      // Set the database instance
      Database.instance = conn.db('grindery-bot');
    }
    // Return the database instance
    return Database.instance;
  }
}
