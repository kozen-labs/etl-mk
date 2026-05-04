import { IConfig, IDependency, IDependencyMap, KzModule } from '@kozen/engine';
import fs from 'fs';
import path from 'path';
import cli from './configs/cli.json';
import ioc from './configs/ioc.json';

export class EtlModule extends KzModule {
  constructor(dependency?: unknown) {
    super(dependency as never);
    this.metadata.alias = 'etl';
    try {
      const pkg = JSON.parse(
        fs.readFileSync(path.resolve(__dirname, '../package.json'), 'utf-8')
      ) as Record<string, string>;
      this.metadata.name    = pkg['name'];
      this.metadata.version = pkg['version'];
      this.metadata.summary = pkg['description'];
      this.metadata.author  = pkg['author'];
      this.metadata.license = pkg['license'];
      this.metadata.uri     = pkg['homepage'];
    } catch (_) {
      // metadata is optional — non-fatal if package.json is unreachable
    }
  }

  public register(
    config: IConfig | null
  ): Promise<Record<string, IDependency> | null> {
    let dep: Record<string, unknown>;
    switch (config?.type) {
      case 'cli': dep = { ...ioc, ...cli }; break;
      default:    dep = { ...ioc };         break;
    }
    return Promise.resolve(this.fix(dep as IDependencyMap) as Record<string, IDependency>);
  }
}

export default EtlModule;

export type { IEtlOptions, IMongoConfig, IKafkaConfig, IMongoToKafkaConfig, IKafkaToMongoConfig } from './models/IEtlOptions';
export type { IEtlMongoToKafkaTools } from './models/IEtlTools';
export type { IKafkaDelegate }        from './models/IEtlDelegate';
export { DelegateLoaderService } from './services/DelegateLoaderService';
export { KafkaProducerService }  from './services/KafkaProducerService';
export { KafkaConsumerService }  from './services/KafkaConsumerService';
export { MongoWriterService }    from './services/MongoWriterService';
export { MongoToKafkaService }   from './services/MongoToKafkaService';
export { KafkaToMongoService }   from './services/KafkaToMongoService';
export { EtlPipelineService }    from './services/EtlPipelineService';
export { EtlCLIController }      from './controllers/EtlCLIController';
