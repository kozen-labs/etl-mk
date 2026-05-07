import { BaseService } from '@kozen/engine';
import { randomUUID } from 'crypto';
import { KafkaJSNonRetriableError } from 'kafkajs';
import type { EachMessagePayload } from 'kafkajs';
import type { ITriggerTools } from '@kozen/trigger';
import type { KafkaConsumerService } from './KafkaConsumerService';
import type { KafkaProducerService } from './KafkaProducerService';
import type { MongoWriterService } from './MongoWriterService';
import type { IKafkaDelegate } from '../models/IEtlDelegate';
import type { IEtlOptions, IKafkaToMongoConfig } from '../models/IEtlOptions';

/**
 * Kafka consumer that writes transformed messages to MongoDB with retry and DLQ routing.
 */
export class KafkaToMongoService extends BaseService {
  private srvKafkaConsumer?: KafkaConsumerService;
  private srvKafkaProducer?: KafkaProducerService;
  private srvMongoWriter?: MongoWriterService;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as never);
    this.srvKafkaConsumer = dependency?.['srvKafkaConsumer'] as KafkaConsumerService;
    this.srvKafkaProducer = dependency?.['srvKafkaProducer'] as KafkaProducerService;
    this.srvMongoWriter   = dependency?.['srvMongoWriter']   as MongoWriterService;
  }

  /**
   * Resolves the delegate via IoC, connects consumer and writer, then enters the message loop.
   */
  async start(options: IEtlOptions): Promise<void> {
    const km = options.km;
    if (!km?.delegate) {
      this.logger?.info({
        flow: options.flow,
        src: 'EtlMk:KafkaToMongo:start',
        message: 'No KM delegate defined. Kafka→MongoDB pipeline will not start.'
      });
      return;
    }

    const delegate = await this.assistant?.get<IKafkaDelegate>(km.delegate);
    if (!delegate) {
      this.logger?.warn({
        flow: options.flow,
        src: 'EtlMk:KafkaToMongo:start',
        message: 'KM delegate could not be resolved. Kafka→MongoDB pipeline will not start.'
      });
      return;
    }

    await this.srvKafkaConsumer?.connect(
      km.source.brokers,
      km.source.groupId        ?? 'etl-mk-group',
      km.source.clientId       ?? 'etl-km',
      km.source.ssl,
      km.source.sessionTimeout,
      km.source.heartbeatInterval
    );
    await this.srvKafkaProducer?.connect(
      km.source.brokers,
      km.source.clientId ?? 'etl-km',
      km.source.ssl
    );
    await this.srvMongoWriter?.connect(km.destination.uri);
    await this.srvKafkaConsumer?.subscribe(km.source.topic, km.source.fromBeginning);

    const db         = this.srvMongoWriter!.getDb(km.destination.database);
    const collection = db.collection(km.destination.collection);

    const tools: ITriggerTools = {
      flow:           options.flow,
      db,
      collection,
      dbName:         km.destination.database,
      collectionName: km.destination.collection,
      assistant:      this.assistant ?? undefined
    };

    this.logger?.info({
      flow: options.flow,
      src: 'EtlMk:KafkaToMongo:start',
      message: 'Pipeline started',
      data: { topic: km.source.topic, database: km.destination.database, collection: km.destination.collection }
    });

    await this.srvKafkaConsumer?.run(
      (payload) => this.onMessage(payload, delegate, tools, km),
      km.source.autoCommit
    );
  }

  /**
   * Retry-then-DLQ loop: commits the offset only after a successful write or DLQ routing.
   */
  async onMessage(
    payload: EachMessagePayload,
    delegate: IKafkaDelegate,
    tools: ITriggerTools,
    km: IKafkaToMongoConfig
  ): Promise<void> {
    const eventFlow    = randomUUID();
    const dlqTopic     = km.dlqTopic      ?? `${km.source.topic}-dlq`;
    const maxAttempts  = km.retryAttempts ?? 3;
    const retryDelayMs = km.retryDelayMs  ?? 1000;
    const nextOffset   = (BigInt(payload.message.offset) + 1n).toString();

    let rawPayload: unknown;
    try {
      rawPayload = JSON.parse(payload.message.value?.toString() ?? '{}');
    } catch {
      rawPayload = payload.message.value?.toString();
    }

    const handler = delegate.message ?? delegate.on ?? delegate.default;

    if (typeof handler !== 'function') {
      this.logger?.warn({
        flow: eventFlow,
        src: 'EtlMk:KafkaToMongo:onMessage',
        message: 'No handler defined in KM delegate'
      });
      await this.commitSafe(payload.topic, payload.partition, nextOffset, eventFlow);
      return;
    }

    let attempts = 0;

    while (true) {
      try {
        const document = await (handler as (
          msg: unknown,
          tools: ITriggerTools
        ) => Promise<unknown>).call(this, rawPayload, { ...tools, flow: eventFlow });

        if (document !== null && document !== undefined) {
          let result =await this.srvMongoWriter?.write(
            km.destination.database,
            km.destination.collection,
            document as Record<string, unknown>,
            km.writeMode ?? 'insert'
          );
          this.logger?.info({
            flow: eventFlow,
            src: 'EtlMk:KafkaToMongo:onMessage',
            message: 'Message written',
            data: { collection: km.destination.collection, database: km.destination.database, result }
          });
        }

        break;

      } catch (error: unknown) {
        attempts++;
        const msg = (error as Error).message;

        if (attempts > maxAttempts) {
          this.logger?.warn({
            flow: eventFlow,
            src: 'EtlMk:KafkaToMongo:onMessage',
            message: `Max retries (${maxAttempts}) exceeded. Routing to DLQ.`,
            data: { dlqTopic, error: msg }
          });
          try {
            await this.srvKafkaProducer?.publishDLQ(dlqTopic, {
              originalMessage: rawPayload,
              error: msg,
              flow: eventFlow,
              timestamp: new Date()
            });
          } catch (dlqError: unknown) {
            this.logger?.error({
              flow: eventFlow,
              src: 'EtlMk:KafkaToMongo:onMessage',
              message: 'DLQ routing failed; message will be skipped.',
              data: { dlqTopic, error: (dlqError as Error).message }
            });
          }
          break;
        }

        this.logger?.warn({
          flow: eventFlow,
          src: 'EtlMk:KafkaToMongo:onMessage',
          message: `Attempt ${attempts}/${maxAttempts} failed, retrying in ${retryDelayMs * attempts}ms`,
          data: { error: msg }
        });
        await this.delay(retryDelayMs * attempts);
      }
    }

    await this.commitSafe(payload.topic, payload.partition, nextOffset, eventFlow);
  }

  /**
   * Commits an offset and absorbs rebalance errors (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION).
   * These errors mean the consumer left the group; the message will be redelivered after rejoin.
   */
  private async commitSafe(topic: string, partition: number, offset: string, flow: string): Promise<void> {
    try {
      await this.srvKafkaConsumer?.commit(topic, partition, offset);
    } catch (error: unknown) {
      if (error instanceof KafkaJSNonRetriableError) {
        this.logger?.warn({
          flow,
          src: 'EtlMk:KafkaToMongo:commit',
          message: 'Offset commit skipped after group rebalance; message will be redelivered.',
          data: { error: (error as Error).message }
        });
        return;
      }
      throw error;
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Disconnects the Kafka consumer and the MongoDB writer.
   */
  async stop(): Promise<void> {
    await this.srvKafkaConsumer?.disconnect();
    await this.srvMongoWriter?.disconnect();
  }
}
