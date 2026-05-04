import { ChangeStreamDocument, Document } from 'mongodb';
import { randomUUID } from 'crypto';
import { ChangeStreamService } from '@kozen/trigger';
import type { ITriggerDelegate, ITriggerTools } from '@kozen/trigger';
import type { KafkaProducerService } from './KafkaProducerService';
import type { IEtlOptions } from '../models/IEtlOptions';
import type { IEtlMongoToKafkaTools } from '../models/IEtlTools';

export class MongoToKafkaService extends ChangeStreamService {
  private srvKafkaProducer?: KafkaProducerService;
  private etlOptions?: IEtlOptions;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as never);
    this.srvKafkaProducer = dependency?.['srvKafkaProducer'] as KafkaProducerService;
  }

  async start(options: IEtlOptions): Promise<void> {
    if (!options.sourceDelegate) {
      this.logger?.info({
        flow: options.flow,
        src: 'EtlMk:MongoToKafka:start',
        message: 'No source delegate defined. MongoDB→Kafka pipeline will not start.'
      });
      return;
    }

    this.etlOptions = options;

    await this.srvKafkaProducer?.connect(
      options.kafka.brokers,
      options.kafka.clientId ?? 'etl-mk',
      options.kafka.ssl
    );

    await super.start({
      flow: options.flow ?? randomUUID(),
      mdb: {
        uri: options.mongo.uri,
        database: options.mongo.database,
        collection: options.mongo.collection
      },
      opt: options.sourceDelegate
    });
  }

  async onChange(
    change: ChangeStreamDocument<Document>,
    delegate?: ITriggerDelegate,
    tools?: ITriggerTools
  ): Promise<void> {
    if (!this.etlOptions || !delegate) return;

    const { kafka, dlqTopic } = this.etlOptions;
    const deadLetterTopic = dlqTopic ?? `${kafka.topic}-dlq`;

    const docKey = (change as unknown as Record<string, unknown>)['documentKey'] as
      | Record<string, unknown>
      | undefined;
    let messageKey = docKey ? String(docKey['_id'] ?? randomUUID()) : randomUUID();
    let messageHeaders: Record<string, string> = {};

    const etlTools: IEtlMongoToKafkaTools = {
      ...(tools ?? {}),
      setMessageKey:     (k) => { messageKey = k; },
      setMessageHeaders: (h) => { messageHeaders = h; }
    } as IEtlMongoToKafkaTools;

    const specificHandler = delegate[change.operationType as keyof ITriggerDelegate];
    const fallback = delegate.on ?? delegate.default;
    const handler = typeof specificHandler === 'function' ? specificHandler : fallback;

    if (typeof handler !== 'function') {
      this.logger?.warn({
        flow: tools?.flow,
        src: 'EtlMk:MongoToKafka:onChange',
        message: `No handler for operation type: ${change.operationType}`
      });
      return;
    }

    const t0 = Date.now();

    try {
      // ITriggerDelegate types handlers as returning void, but ETL handlers return the Kafka payload.
      const payload = await (handler as unknown as (
        c: typeof change,
        t: IEtlMongoToKafkaTools
      ) => Promise<unknown>).call(this, change, etlTools);

      if (payload !== null && payload !== undefined) {
        await this.srvKafkaProducer?.publish(kafka.topic, messageKey, payload, messageHeaders);
        this.logger?.info({
          flow: tools?.flow,
          src: 'EtlMk:MongoToKafka:onChange',
          message: 'Event published',
          data: { operationType: change.operationType, topic: kafka.topic, durationMs: Date.now() - t0 }
        });
      }
    } catch (error: unknown) {
      const msg = (error as Error).message;
      this.logger?.warn({
        flow: tools?.flow,
        src: 'EtlMk:MongoToKafka:onChange',
        message: 'Event failed, routing to DLQ',
        data: { dlqTopic: deadLetterTopic, error: msg }
      });
      await this.srvKafkaProducer?.publishDLQ(deadLetterTopic, {
        originalPayload: change,
        error: msg,
        flow: tools?.flow,
        timestamp: new Date()
      });
    }
  }

  async stop(): Promise<void> {
    await this.srvKafkaProducer?.disconnect();
    await super.stop();
  }
}
