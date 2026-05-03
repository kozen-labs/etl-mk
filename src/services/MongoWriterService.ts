import { BaseService } from '@kozen/engine';
import { MongoClient, Db } from 'mongodb';

export class MongoWriterService extends BaseService {
  private client: MongoClient | null = null;

  async connect(uri: string): Promise<void> {
    this.client = new MongoClient(uri);
    await this.client.connect();

    this.logger?.info({
      src: 'EtlMk:MongoWriter:connect',
      message: `MongoDB writer connected`
    });
  }

  getDb(dbName: string): Db {
    if (!this.client) throw new Error('MongoWriterService: not connected');
    return this.client.db(dbName);
  }

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

  async writeDLQ(dbName: string, dlqCollection: string, payload: unknown): Promise<void> {
    await this.getDb(dbName).collection(dlqCollection).insertOne(
      payload as Record<string, unknown>
    );
  }

  async disconnect(): Promise<void> {
    await this.client?.close();
    this.client = null;
  }
}
