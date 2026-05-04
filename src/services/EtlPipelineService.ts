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
    const hasMk = !!options.mk?.delegate;
    const hasKm = !!options.km?.delegate;

    if (!hasMk && !hasKm) {
      this.logger?.warn({
        flow: options.flow,
        src: 'EtlMk:Pipeline:start',
        message: 'No delegates defined. Set ETL_MK_DELEGATE_FILE and/or ETL_KM_DELEGATE_FILE.'
      });
      return { await: false };
    }

    await Promise.all([
      hasMk ? this.srvMongoToKafka?.start(options) : Promise.resolve(),
      hasKm ? this.srvKafkaToMongo?.start(options) : Promise.resolve()
    ]);

    return { await: true };
  }
}
