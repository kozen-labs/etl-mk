import path from 'node:path';
import { CLIController, FileService, IArgs, IIoC, ILogger, IModule } from '@kozen/engine';
import type { EtlPipelineService } from '../services/EtlPipelineService';
import type { IEtlOptions, IMongoConfig, IKafkaConfig } from '../models/IEtlOptions';

export class EtlCLIController extends CLIController {
  private srvPipeline?: EtlPipelineService;

  constructor(dependency?: { assistant: IIoC; logger: ILogger; srvPipeline?: EtlPipelineService; srvFile?: FileService }) {
    super(dependency);
    this.srvPipeline = dependency?.srvPipeline;
  }

  async help(): Promise<void> {
    const mod = await this.assistant?.get<IModule>('module:@kozen/etl-mk');
    const dir = process.env['DOCS_DIR'] ?? path.resolve(__dirname, '../docs');
    const body = await this.srvFile?.select('etl-mk', dir);
    super.help({
      title:   `'${mod?.metadata?.alias ?? 'etl'}' from '${mod?.metadata?.name ?? '@kozen/etl-mk'}'`,
      body,
      version: mod?.metadata?.version,
      uri:     mod?.metadata?.uri
    });
  }

  async fill(args: string[] | IArgs): Promise<IArgs> {
    const parsed = this.extract(args) as IArgs & Record<string, unknown>;

    // MongoDB connection
    parsed['mongo'] = parsed['mongo'] ?? {};
    const mongo = parsed['mongo'] as Record<string, unknown>;
    mongo['uri']        = mongo['uri']        ?? parsed['mongo.uri']        ?? process.env['ETL_MONGO_URI'];
    mongo['database']   = mongo['database']   ?? parsed['mongo.database']   ?? process.env['ETL_MONGO_DATABASE'];
    mongo['collection'] = mongo['collection'] ?? parsed['mongo.collection'] ?? process.env['ETL_MONGO_COLLECTION'];

    // Kafka connection
    parsed['kafka'] = parsed['kafka'] ?? {};
    const kafka = parsed['kafka'] as Record<string, unknown>;
    kafka['brokers']  = kafka['brokers']  ?? (process.env['ETL_KAFKA_BROKERS']?.split(','));
    kafka['topic']    = kafka['topic']    ?? parsed['kafka.topic']    ?? process.env['ETL_KAFKA_TOPIC'];
    kafka['groupId']  = kafka['groupId']  ?? parsed['kafka.groupId']  ?? process.env['ETL_KAFKA_GROUP_ID']  ?? 'etl-mk-group';
    kafka['clientId'] = kafka['clientId'] ?? parsed['kafka.clientId'] ?? process.env['ETL_KAFKA_CLIENT_ID'] ?? 'etl-mk';
    kafka['ssl']      = kafka['ssl']      ?? (process.env['ETL_KAFKA_SSL'] === 'true');

    // Delegate type shared between source and destination
    const moduleType = parsed['delegateType'] as string | undefined
      ?? process.env['ETL_DELEGATE_TYPE'];

    // Source delegate (MongoDB → Kafka)
    const srcFile = parsed['sourceDelegateFile'] as string | undefined
      ?? process.env['ETL_SOURCE_DELEGATE_FILE'];
    const srcKey  = parsed['sourceDelegateKey']  as string | undefined
      ?? process.env['ETL_SOURCE_DELEGATE_KEY']
      ?? 'etl-mk:delegate:source';
    if (srcFile) {
      parsed['sourceDelegate'] = { key: srcKey, file: srcFile, type: 'instance', moduleType };
    }

    // Destination delegate (Kafka → MongoDB)
    const dstFile = parsed['destinationDelegateFile'] as string | undefined
      ?? process.env['ETL_DESTINATION_DELEGATE_FILE'];
    const dstKey  = parsed['destinationDelegateKey']  as string | undefined
      ?? process.env['ETL_DESTINATION_DELEGATE_KEY']
      ?? 'etl-mk:delegate:destination';
    if (dstFile) {
      parsed['destinationDelegate'] = { key: dstKey, file: dstFile, type: 'instance', moduleType };
    }

    // Pipeline options
    parsed['writeMode']     = parsed['writeMode']     ?? process.env['ETL_WRITE_MODE']     ?? 'insert';
    parsed['dlqTopic']      = parsed['dlqTopic']      ?? process.env['ETL_DLQ_TOPIC'];
    parsed['retryAttempts'] = parsed['retryAttempts'] ?? Number(process.env['ETL_RETRY_ATTEMPTS'] ?? 3);
    parsed['retryDelayMs']  = parsed['retryDelayMs']  ?? Number(process.env['ETL_RETRY_DELAY_MS']  ?? 1000);

    return parsed;
  }

  async start(args?: string[] | IArgs): Promise<{ await: boolean }> {
    const flow = this.getId(args as never);
    try {
      const filled  = await this.fill((args ?? []) as IArgs);
      const options = this.buildOptions(filled as unknown as Record<string, unknown>);
      this.logger?.info({
        flow,
        src: 'EtlMk:CLI:start',
        message: 'Starting ETL pipeline',
        data: {
          sourceDelegate:      !!options.sourceDelegate,
          destinationDelegate: !!options.destinationDelegate
        }
      });
      return await this.srvPipeline?.start(options) ?? { await: false };
    } catch (error: unknown) {
      this.logger?.error({
        flow,
        src: 'EtlMk:CLI:start',
        message: `Failed to start ETL pipeline: ${(error as Error).message}`
      });
      return { await: false };
    }
  }

  async validate(args?: string[] | IArgs): Promise<void> {
    const flow   = this.getId(args as never);
    const filled = await this.fill((args ?? []) as IArgs);
    const missing: string[] = [];

    const mongo = filled['mongo'] as Record<string, unknown>;
    const kafka  = filled['kafka'] as Record<string, unknown>;

    if (!mongo?.['uri'])        missing.push('ETL_MONGO_URI');
    if (!mongo?.['database'])   missing.push('ETL_MONGO_DATABASE');
    if (!mongo?.['collection']) missing.push('ETL_MONGO_COLLECTION');
    if (!kafka?.['brokers'])    missing.push('ETL_KAFKA_BROKERS');
    if (!kafka?.['topic'])      missing.push('ETL_KAFKA_TOPIC');

    const hasSource = !!(filled['sourceDelegate'] ?? process.env['ETL_SOURCE_DELEGATE_FILE']);
    const hasDest   = !!(filled['destinationDelegate'] ?? process.env['ETL_DESTINATION_DELEGATE_FILE']);
    if (!hasSource && !hasDest) {
      missing.push('ETL_SOURCE_DELEGATE_FILE or ETL_DESTINATION_DELEGATE_FILE');
    }

    if (missing.length > 0) {
      this.logger?.error({ flow, src: 'EtlMk:CLI:validate', message: 'Configuration incomplete', data: { missing } });
      throw new Error(`Missing required configuration: ${missing.join(', ')}`);
    }

    this.logger?.info({ flow, src: 'EtlMk:CLI:validate', message: 'Configuration valid' });
  }

  private buildOptions(filled: Record<string, unknown>): IEtlOptions {
    return {
      mongo:               filled['mongo']               as IMongoConfig,
      kafka:               filled['kafka']               as IKafkaConfig,
      sourceDelegate:      filled['sourceDelegate']      as IEtlOptions['sourceDelegate'],
      destinationDelegate: filled['destinationDelegate'] as IEtlOptions['destinationDelegate'],
      writeMode:           filled['writeMode']           as IEtlOptions['writeMode'],
      dlqTopic:            filled['dlqTopic']            as string | undefined,
      retryAttempts:       filled['retryAttempts']       as number | undefined,
      retryDelayMs:        filled['retryDelayMs']        as number | undefined
    };
  }
}
