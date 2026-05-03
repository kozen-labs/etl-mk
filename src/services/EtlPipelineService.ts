import { BaseService } from '@kozen/engine';
import type { MongoToKafkaService } from './MongoToKafkaService';
import type { KafkaToMongoService } from './KafkaToMongoService';
import type { IEtlOptions } from '../models/IEtlOptions';

export class EtlPipelineService extends BaseService {
  private srvMongoToKafka?: MongoToKafkaService;
  private srvKafkaToMongo?: KafkaToMongoService;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as { assistant: never; logger: never });
    this.srvMongoToKafka = dependency?.['srvMongoToKafka'] as MongoToKafkaService;
    this.srvKafkaToMongo = dependency?.['srvKafkaToMongo'] as KafkaToMongoService;
  }

  async start(options: IEtlOptions): Promise<{ await: boolean }> {
    switch (options.mode) {
      case 'mongo-to-kafka':
        await this.srvMongoToKafka?.start(options);
        return { await: true };

      case 'kafka-to-mongo':
        await this.srvKafkaToMongo?.start(options);
        return { await: true };

      default:
        throw new Error(
          `Unknown ETL mode: '${options.mode}'. Use 'mongo-to-kafka' or 'kafka-to-mongo'.`
        );
    }
  }
}
