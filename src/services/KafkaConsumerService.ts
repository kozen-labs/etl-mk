import { BaseService } from '@kozen/engine';
import { Kafka, Consumer, EachMessagePayload, logLevel } from 'kafkajs';

export class KafkaConsumerService extends BaseService {
  private consumer: Consumer | null = null;

  async connect(brokers: string[], groupId: string, clientId: string, ssl = false): Promise<void> {
    const kafka = new Kafka({ clientId, brokers, ssl, logLevel: logLevel.NOTHING });
    this.consumer = kafka.consumer({ groupId });
    await this.consumer.connect();
    this.logger?.info({
      src: 'EtlMk:KafkaConsumer:connect',
      message: 'Kafka consumer connected',
      data: { brokers, groupId, clientId }
    });
  }

  async subscribe(topic: string): Promise<void> {
    if (!this.consumer) throw new Error('KafkaConsumerService: not connected');
    await this.consumer.subscribe({ topic, fromBeginning: false });
  }

  async run(handler: (payload: EachMessagePayload) => Promise<void>): Promise<void> {
    if (!this.consumer) throw new Error('KafkaConsumerService: not connected');
    await this.consumer.run({ autoCommit: false, eachMessage: handler });
  }

  async commit(topic: string, partition: number, offset: string): Promise<void> {
    await this.consumer?.commitOffsets([{ topic, partition, offset }]);
  }

  async disconnect(): Promise<void> {
    await this.consumer?.disconnect();
    this.consumer = null;
  }
}
