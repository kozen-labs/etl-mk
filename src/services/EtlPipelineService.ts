import { BaseService } from '@kozen/engine';
import type { MongoToKafkaService } from './MongoToKafkaService';
import type { KafkaToMongoService } from './KafkaToMongoService';
import type { IEtlOptions } from '../models/IEtlOptions';

export class EtlPipelineService extends BaseService {
  private srvMongoToKafka?: MongoToKafkaService;
  private srvKafkaToMongo?: KafkaToMongoService;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as never);
    this.srvMongoToKafka = dependency?.['srvMongoToKafka'] as MongoToKafkaService;
    this.srvKafkaToMongo = dependency?.['srvKafkaToMongo'] as KafkaToMongoService;
  }

  async start(options: IEtlOptions): Promise<{ await: boolean }> {
    const hasSource = !!options.sourceDelegate;
    const hasDest   = !!options.destinationDelegate;

    if (!hasSource && !hasDest) {
      this.logger?.warn({
        flow: options.flow,
        src: 'EtlMk:Pipeline:start',
        message: 'No delegates defined. Set ETL_SOURCE_DELEGATE_FILE and/or ETL_DESTINATION_DELEGATE_FILE.'
      });
      return { await: false };
    }

    await Promise.all([
      hasSource ? this.srvMongoToKafka?.start(options) : Promise.resolve(),
      hasDest   ? this.srvKafkaToMongo?.start(options) : Promise.resolve()
    ]);

    return { await: true };
  }
}
