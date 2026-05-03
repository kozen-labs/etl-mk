import { BaseService } from '@kozen/engine';
import { MongoClient } from 'mongodb';
import { randomUUID } from 'crypto';
import type { DelegateLoaderService } from './DelegateLoaderService';
import type { KafkaProducerService } from './KafkaProducerService';
import type { IEtlOptions, IEtlSourceMongo, IEtlDestinationKafka } from '../models/IEtlOptions';
import type { IEtlMongoToKafkaTools } from '../models/IEtlTools';

export class MongoToKafkaService extends BaseService {
  private srvDelegateLoader?: DelegateLoaderService;
  private srvKafkaProducer?: KafkaProducerService;
  private client?: MongoClient;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as { assistant: never; logger: never });
    this.srvDelegateLoader = dependency?.['srvDelegateLoader'] as DelegateLoaderService;
    this.srvKafkaProducer  = dependency?.['srvKafkaProducer']  as KafkaProducerService;
  }

  async start(options: IEtlOptions): Promise<void> {
    const source = options.source as IEtlSourceMongo;
    const dest   = options.destination as IEtlDestinationKafka;
    const flow   = randomUUID();
    const dlqTopic = dest.dlqTopic ?? `${dest.topic}-dlq`;

    await this.srvDelegateLoader?.load(options.delegateFile, options.delegateType);
    await this.srvKafkaProducer?.connect(dest.brokers, dest.clientId ?? 'etl-mk', dest.ssl);

    this.client = new MongoClient(source.uri);
    await this.client.connect();

    const db         = this.client.db(source.database);
    const collection = db.collection(source.collection);

    this.logger?.info({
      flow,
      src: 'EtlMk:MongoToKafka:start',
      message: `Pipeline started`,
      data: { database: source.database, collection: source.collection, topic: dest.topic }
    });

    const changeStream = collection.watch();

    changeStream.on('change', async (rawChange) => {
      const change = rawChange as unknown as Record<string, unknown>;
      const eventFlow = randomUUID();
      const docKey = change['documentKey'] as Record<string, unknown> | undefined;
      let messageKey = docKey ? String(docKey['_id'] ?? eventFlow) : eventFlow;
      let messageHeaders: Record<string, string> = {};

      const tools: IEtlMongoToKafkaTools = {
        mode: 'mongo-to-kafka',
        flow: eventFlow,
        db,
        collection,
        dbName: source.database,
        collectionName: source.collection,
        assistant: this.assistant ?? undefined,
        setMessageKey:     (k) => { messageKey = k; },
        setMessageHeaders: (h) => { messageHeaders = h; }
      };

      const t0 = Date.now();

      try {
        const operationType = change['operationType'] as string;
        const payload = await this.srvDelegateLoader?.dispatch(change, tools, operationType);

        if (payload !== null && payload !== undefined) {
          await this.srvKafkaProducer?.publish(dest.topic, messageKey, payload);
          void messageHeaders;

          this.logger?.info({
            flow: eventFlow,
            src: 'EtlMk:MongoToKafka:change',
            message: `Event published`,
            data: { operationType, topic: dest.topic, durationMs: Date.now() - t0 }
          });
        }
      } catch (error: unknown) {
        const msg = (error as Error).message;
        this.logger?.warn({
          flow: eventFlow,
          src: 'EtlMk:MongoToKafka:change',
          message: `Event failed, routing to DLQ`,
          data: { dlqTopic, error: msg }
        });

        await this.srvKafkaProducer?.publishDLQ(dlqTopic, {
          originalPayload: rawChange,
          error: msg,
          flow: eventFlow,
          timestamp: new Date()
        });
      }
    });

    changeStream.on('error', (error) => {
      this.logger?.error({
        flow,
        src: 'EtlMk:MongoToKafka:changeStream',
        message: `Change stream error: ${error.message}`
      });
    });
  }

  async stop(): Promise<void> {
    await this.srvKafkaProducer?.disconnect();
    await this.client?.close();
  }
}
