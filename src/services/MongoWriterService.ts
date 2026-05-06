import { BaseService } from '@kozen/engine';
import { MongoClient, Db } from 'mongodb';

/**
 * Manages a MongoClient for document writes; one instance per KM pipeline run (transient).
 */
export class MongoWriterService extends BaseService {
  private client: MongoClient | null = null;

  /**
   * Opens the MongoClient connection; must be called before write or getDb.
   */
  async connect(uri: string): Promise<void> {
    this.client = new MongoClient(uri);
    await this.client.connect();

    this.logger?.info({
      src: 'EtlMk:MongoWriter:connect',
      message: `MongoDB writer connected`
    });
  }

  /**
   * Returns the Db handle for the given name; throws if not connected.
   */
  getDb(dbName: string): Db {
    if (!this.client) throw new Error('MongoWriterService: not connected');
    return this.client.db(dbName);
  }

  /**
   * Upserts on _id when writeMode is 'upsert'; falls back to insertOne otherwise.
   */
  async write(
    dbName: string,
    collectionName: string,
    document: Record<string, unknown>,
    writeMode: 'insert' | 'upsert' = 'insert'
  ): Promise<void> {
    const collection = this.getDb(dbName).collection(collectionName);

    if (writeMode === 'upsert' && document['_id']) {
      const { _id, ...rest } = document;
      await collection.updateOne({ _id }, { $set: rest }, { upsert: true });
    } else {
      await collection.insertOne(document);
    }
  }

  /**
   * Inserts a dead-letter record into a MongoDB collection when Kafka DLQ is unavailable.
   */
  async writeDLQ(dbName: string, dlqCollection: string, payload: unknown): Promise<void> {
    await this.getDb(dbName).collection(dlqCollection).insertOne(
      payload as Record<string, unknown>
    );
  }

  /**
   * Closes the MongoClient; safe to call even if connect was never called.
   */
  async disconnect(): Promise<void> {
    await this.client?.close();
    this.client = null;
  }
}
