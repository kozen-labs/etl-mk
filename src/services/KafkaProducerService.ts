import { BaseService } from '@kozen/engine';
import { Kafka, Producer, logLevel } from 'kafkajs';

/**
 * Manages a single KafkaJS producer connection; one instance per pipeline run (transient).
 */
export class KafkaProducerService extends BaseService {
  private producer: Producer | null = null;

  /**
   * Creates and connects the KafkaJS producer; must be called before publish.
   */
  async connect(brokers: string[], clientId: string, ssl = false): Promise<void> {
    const kafka = new Kafka({ clientId, brokers, ssl, logLevel: logLevel.NOTHING });
    this.producer = kafka.producer();
    await this.producer.connect();

    this.logger?.info({
      src: 'EtlMk:KafkaProducer:connect',
      message: `Kafka producer connected`,
      data: { brokers, clientId }
    });
  }

  /**
   * Publishes a single JSON-serialised message; headers are Buffer-encoded for KafkaJS.
   */
  async publish(
    topic: string,
    key: string,
    value: unknown,
    headers?: Record<string, string>
  ): Promise<void> {
    if (!this.producer) throw new Error('KafkaProducerService: not connected');

    const kafkaHeaders = headers
      ? Object.fromEntries(Object.entries(headers).map(([k, v]) => [k, Buffer.from(v)]))
      : undefined;

    await this.producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(value), headers: kafkaHeaders }]
    });
  }

  /**
   * Routes a failed payload to the dead-letter topic without a message key.
   */
  async publishDLQ(dlqTopic: string, payload: unknown): Promise<void> {
    if (!this.producer) throw new Error('KafkaProducerService: not connected');

    await this.producer.send({
      topic: dlqTopic,
      messages: [{ value: JSON.stringify(payload) }]
    });
  }

  /**
   * Disconnects the producer; safe to call even if connect was never called.
   */
  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }
}
