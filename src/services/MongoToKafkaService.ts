import { ChangeStreamDocument, Document } from 'mongodb';
import { randomUUID } from 'crypto';
import { ChangeStreamService } from '@kozen/trigger';
import type { ITriggerDelegate, ITriggerTools } from '@kozen/trigger';
import type { KafkaProducerService } from './KafkaProducerService';
import type { IEtlOptions, IMongoToKafkaConfig } from '../models/IEtlOptions';
import type { IEtlMongoToKafkaTools } from '../models/IEtlTools';

/**
 * MongoDB change stream consumer that transforms events and publishes them to Kafka.
 */
export class MongoToKafkaService extends ChangeStreamService {
  private srvKafkaProducer?: KafkaProducerService;
  private mk?: IMongoToKafkaConfig;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as never);
    this.srvKafkaProducer = dependency?.['srvKafkaProducer'] as KafkaProducerService;
  }

  /**
   * Connects the Kafka producer then delegates to ChangeStreamService for cursor lifecycle.
   */
  async start(options: IEtlOptions): Promise<void> {
    const mk = options.mk;
    if (!mk?.delegate) {
      this.logger?.info({
        flow: options.flow,
        src: 'EtlMk:MongoToKafka:start',
        message: 'No MK delegate defined. MongoDB→Kafka pipeline will not start.'
      });
      return;
    }

    this.mk = mk;

    try {
      await this.srvKafkaProducer?.connect(
        mk.destination.brokers,
        mk.destination.clientId ?? 'etl-mk',
        mk.destination.ssl
      );
    } catch (error: unknown) {
      this.logger?.error({
        flow: options.flow,
        src: 'EtlMk:MongoToKafka:start',
        message: 'Failed to connect to Kafka. MongoDB→Kafka pipeline will not start.',
        data: { error: (error as Error).message }
      });
      throw new Error('Kafka connection failed' + (error instanceof Error ? `: ${error.message}` : ''));
    }

    try {
      await super.start({
        flow: options.flow ?? randomUUID(),
        mdb: {
          uri: mk.source.uri,
          database: mk.source.database,
          collection: mk.source.collection
        },
        opt: mk.delegate
      });
    } catch (error: unknown) {
      this.logger?.error({
        flow: options.flow,
        src: 'EtlMk:MongoToKafka:start',
        message: 'Failed to start MongoDB change stream.',
        data: { error: (error as Error).message }
      });
      await this.srvKafkaProducer?.disconnect();
      throw new Error('Failed to start MongoDB change stream' + (error instanceof Error ? `: ${error.message}` : ''));
    }
  }

  /**
   * Captures the delegate's return value as the Kafka payload; null/undefined skips publish.
   */
  async onChange(
    change: ChangeStreamDocument<Document>,
    delegate?: ITriggerDelegate,
    tools?: ITriggerTools
  ): Promise<void> {
    const mk = this.mk;
    if (!mk || !delegate) return;

    const deadLetterTopic = mk.dlqTopic ?? `${mk.destination.topic}-dlq`;

    const docKey = (change as unknown as Record<string, unknown>)['documentKey'] as
      | Record<string, unknown>
      | undefined;
    let messageKey = docKey ? String(docKey['_id'] ?? randomUUID()) : randomUUID();
    let messageHeaders: Record<string, string> = {};

    const etlTools: IEtlMongoToKafkaTools = {
      ...(tools ?? {}),
      setMessageKey: (k) => { messageKey = k; },
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
      // ITriggerDelegate types handlers as void, but ETL handlers return the Kafka payload.
      const payload = await (handler as unknown as (
        c: typeof change,
        t: IEtlMongoToKafkaTools
      ) => Promise<unknown>).call(this, change, etlTools);

      if (payload !== null && payload !== undefined) {
        await this.srvKafkaProducer?.publish(mk.destination.topic, messageKey, payload, messageHeaders);
        this.logger?.info({
          flow: tools?.flow,
          src: 'EtlMk:MongoToKafka:onChange',
          message: 'Event published',
          data: { operationType: change.operationType, topic: mk.destination.topic, durationMs: Date.now() - t0 }
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

  /**
   * Disconnects the Kafka producer and stops the change stream cursor.
   */
  async stop(): Promise<void> {
    await this.srvKafkaProducer?.disconnect();
    await super.stop();
  }
}
