import { BaseService } from '@kozen/engine';
import { randomUUID } from 'crypto';
import type { DelegateLoaderService } from './DelegateLoaderService';
import type { KafkaConsumerService } from './KafkaConsumerService';
import type { MongoWriterService } from './MongoWriterService';
import type { IEtlOptions, IEtlSourceKafka, IEtlDestinationMongo } from '../models/IEtlOptions';
import type { IEtlKafkaToMongoTools } from '../models/IEtlTools';

export class KafkaToMongoService extends BaseService {
  private srvDelegateLoader?: DelegateLoaderService;
  private srvKafkaConsumer?: KafkaConsumerService;
  private srvMongoWriter?: MongoWriterService;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as { assistant: never; logger: never });
    this.srvDelegateLoader = dependency?.['srvDelegateLoader'] as DelegateLoaderService;
    this.srvKafkaConsumer  = dependency?.['srvKafkaConsumer']  as KafkaConsumerService;
    this.srvMongoWriter    = dependency?.['srvMongoWriter']    as MongoWriterService;
  }

  async start(options: IEtlOptions): Promise<void> {
    const source = options.source as IEtlSourceKafka;
    const dest   = options.destination as IEtlDestinationMongo;
    const flow   = randomUUID();
    const dlqCollection = dest.dlqCollection ?? `${dest.collection}_dlq`;

    await this.srvDelegateLoader?.load(options.delegateFile, options.delegateType);

    await this.srvKafkaConsumer?.connect(
      source.brokers,
      source.groupId  ?? 'etl-mk-group',
      source.clientId ?? 'etl-mk',
      source.ssl
    );

    await this.srvMongoWriter?.connect(dest.uri);

    await this.srvKafkaConsumer?.subscribe(source.topic);

    this.logger?.info({
      flow,
      src: 'EtlMk:KafkaToMongo:start',
      message: `Pipeline started`,
      data: { topic: source.topic, database: dest.database, collection: dest.collection }
    });

    await this.srvKafkaConsumer?.run(async ({ message }) => {
      const eventFlow = randomUUID();

      let payload: unknown;
      try {
        payload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        payload = message.value?.toString();
      }

      const db         = this.srvMongoWriter!.getDb(dest.database);
      const collection = db.collection(dest.collection);

      const tools: IEtlKafkaToMongoTools = {
        mode: 'kafka-to-mongo',
        flow: eventFlow,
        db,
        collection,
        dbName: dest.database,
        collectionName: dest.collection,
        assistant: this.assistant ?? undefined
      };

      const t0 = Date.now();

      try {
        const document = await this.srvDelegateLoader?.dispatch(payload, tools);

        if (document !== null && document !== undefined) {
          await this.srvMongoWriter?.write(
            dest.database,
            dest.collection,
            document as Record<string, unknown>,
            dest.writeMode ?? 'insert'
          );

          this.logger?.info({
            flow: eventFlow,
            src: 'EtlMk:KafkaToMongo:message',
            message: `Message written`,
            data: { collection: dest.collection, durationMs: Date.now() - t0 }
          });
        }
      } catch (error: unknown) {
        const msg = (error as Error).message;
        this.logger?.warn({
          flow: eventFlow,
          src: 'EtlMk:KafkaToMongo:message',
          message: `Message failed, routing to DLQ`,
          data: { dlqCollection, error: msg }
        });

        await this.srvMongoWriter?.writeDLQ(dest.database, dlqCollection, {
          originalMessage: payload,
          error: msg,
          flow: eventFlow,
          timestamp: new Date()
        });
      }
    });
  }

  async stop(): Promise<void> {
    await this.srvKafkaConsumer?.disconnect();
    await this.srvMongoWriter?.disconnect();
  }
}
