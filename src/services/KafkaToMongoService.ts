import { BaseService } from '@kozen/engine';
import { randomUUID } from 'crypto';
import type { EachMessagePayload } from 'kafkajs';
import type { ITriggerTools } from '@kozen/trigger';
import type { KafkaConsumerService } from './KafkaConsumerService';
import type { KafkaProducerService } from './KafkaProducerService';
import type { MongoWriterService } from './MongoWriterService';
import type { IKafkaDelegate } from '../models/IEtlDelegate';
import type { IEtlOptions } from '../models/IEtlOptions';

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

  async start(options: IEtlOptions): Promise<void> {
    if (!options.destinationDelegate) {
      this.logger?.info({
        flow: options.flow,
        src: 'EtlMk:KafkaToMongo:start',
        message: 'No destination delegate defined. Kafka→MongoDB pipeline will not start.'
      });
      return;
    }

    const delegate = await this.assistant?.get<IKafkaDelegate>(options.destinationDelegate);
    if (!delegate) {
      this.logger?.warn({
        flow: options.flow,
        src: 'EtlMk:KafkaToMongo:start',
        message: 'Destination delegate could not be resolved. Kafka→MongoDB pipeline will not start.'
      });
      return;
    }

    const { kafka, mongo } = options;

    await this.srvKafkaConsumer?.connect(
      kafka.brokers,
      kafka.groupId  ?? 'etl-mk-group',
      kafka.clientId ?? 'etl-mk',
      kafka.ssl
    );
    await this.srvMongoWriter?.connect(mongo.uri);
    await this.srvKafkaConsumer?.subscribe(kafka.topic);

    const db         = this.srvMongoWriter!.getDb(mongo.database);
    const collection = db.collection(mongo.collection);

    const tools: ITriggerTools = {
      flow: options.flow,
      db,
      collection,
      dbName: mongo.database,
      collectionName: mongo.collection,
      assistant: this.assistant ?? undefined
    };

    this.logger?.info({
      flow: options.flow,
      src: 'EtlMk:KafkaToMongo:start',
      message: 'Pipeline started',
      data: { topic: kafka.topic, database: mongo.database, collection: mongo.collection }
    });

    await this.srvKafkaConsumer?.run(
      (payload) => this.onMessage(payload, delegate, tools, options)
    );
  }

  async onMessage(
    payload: EachMessagePayload,
    delegate: IKafkaDelegate,
    tools: ITriggerTools,
    options: IEtlOptions
  ): Promise<void> {
    const eventFlow    = randomUUID();
    const { mongo, kafka } = options;
    const dlqTopic     = options.dlqTopic ?? `${kafka.topic}-dlq`;
    const maxAttempts  = options.retryAttempts ?? 3;
    const retryDelayMs = options.retryDelayMs  ?? 1000;

    let rawPayload: unknown;
    try {
      rawPayload = JSON.parse(payload.message.value?.toString() ?? '{}');
    } catch {
      rawPayload = payload.message.value?.toString();
    }

    const handler = delegate.message ?? delegate.on ?? delegate.default;
    const nextOffset = (BigInt(payload.message.offset) + 1n).toString();

    if (typeof handler !== 'function') {
      this.logger?.warn({
        flow: eventFlow,
        src: 'EtlMk:KafkaToMongo:onMessage',
        message: 'No handler defined in destination delegate'
      });
      await this.srvKafkaConsumer?.commit(payload.topic, payload.partition, nextOffset);
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
          await this.srvMongoWriter?.write(
            mongo.database,
            mongo.collection,
            document as Record<string, unknown>,
            options.writeMode ?? 'insert'
          );
          this.logger?.info({
            flow: eventFlow,
            src: 'EtlMk:KafkaToMongo:onMessage',
            message: 'Message written',
            data: { collection: mongo.collection }
          });
        }

        await this.srvKafkaConsumer?.commit(payload.topic, payload.partition, nextOffset);
        return;

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
          await this.srvKafkaProducer?.publishDLQ(dlqTopic, {
            originalMessage: rawPayload,
            error: msg,
            flow: eventFlow,
            timestamp: new Date()
          });
          await this.srvKafkaConsumer?.commit(payload.topic, payload.partition, nextOffset);
          return;
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
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async stop(): Promise<void> {
    await this.srvKafkaConsumer?.disconnect();
    await this.srvMongoWriter?.disconnect();
  }
}
