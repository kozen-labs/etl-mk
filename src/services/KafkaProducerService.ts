import { BaseService } from '@kozen/engine';
import { Kafka, Producer, logLevel } from 'kafkajs';

export class KafkaProducerService extends BaseService {
  private producer: Producer | null = null;

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

  async publishDLQ(dlqTopic: string, payload: unknown): Promise<void> {
    if (!this.producer) throw new Error('KafkaProducerService: not connected');

    await this.producer.send({
      topic: dlqTopic,
      messages: [{ value: JSON.stringify(payload) }]
    });
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }
}
