import { BaseService } from '@kozen/engine';
import { Kafka, Consumer, EachMessagePayload, logLevel } from 'kafkajs';

/**
 * Manages a single KafkaJS consumer with manual offset commit for at-least-once delivery.
 */
export class KafkaConsumerService extends BaseService {
  private consumer: Consumer | null = null;

  /**
   * Creates and connects the KafkaJS consumer with the given group and broker list.
   */
  async connect(
    brokers: string[],
    groupId: string,
    clientId: string,
    ssl = false,
    sessionTimeout = 60000,
    heartbeatInterval = 5000
  ): Promise<void> {
    const kafka = new Kafka({ clientId, brokers, ssl, logLevel: logLevel.NOTHING });
    this.consumer = kafka.consumer({ groupId, sessionTimeout, heartbeatInterval });
    await this.consumer.connect();
    this.logger?.info({
      src: 'EtlMk:KafkaConsumer:connect',
      message: 'Kafka consumer connected',
      data: { brokers, groupId, clientId, sessionTimeout, heartbeatInterval }
    });
  }

  /**
   * Subscribes to the topic from the latest offset; must be called after connect.
   */
  async subscribe(topic: string): Promise<void> {
    if (!this.consumer) throw new Error('KafkaConsumerService: not connected');
    await this.consumer.subscribe({ topic, fromBeginning: true });
  }

  /**
   * Starts the consumption loop with autoCommit disabled; commit must be called after each message.
   */
  async run(handler: (payload: EachMessagePayload) => Promise<void>): Promise<void> {
    if (!this.consumer) throw new Error('KafkaConsumerService: not connected');
    await this.consumer.run({ autoCommit: false, eachMessage: handler });
  }

  /**
   * Commits a specific offset; called only after a successful write or DLQ routing.
   */
  async commit(topic: string, partition: number, offset: string): Promise<void> {
    await this.consumer?.commitOffsets([{ topic, partition, offset }]);
  }

  /**
   * Disconnects the consumer; safe to call even if connect was never called.
   */
  async disconnect(): Promise<void> {
    await this.consumer?.disconnect();
    this.consumer = null;
  }
}
