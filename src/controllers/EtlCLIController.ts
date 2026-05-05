import path from 'node:path';
import { CLIController, FileService, IArgs, IIoC, ILogger, IModule } from '@kozen/engine';
import type { EtlPipelineService } from '../services/EtlPipelineService';
import type { IEtlOptions, IMongoToKafkaConfig, IKafkaToMongoConfig } from '../models/IEtlOptions';

/**
 * CLI controller for the ETL module — dispatches etl:start, etl:validate, etl:help.
 */
export class EtlCLIController extends CLIController {
  private srvPipeline?: EtlPipelineService;

  constructor(dependency?: { assistant: IIoC; logger: ILogger; srvPipeline?: EtlPipelineService; srvFile?: FileService }) {
    super(dependency);
    this.srvPipeline = dependency?.srvPipeline;
  }

  /**
   * Renders the inline help text from src/docs/etl-mk.txt.
   */
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

  /**
   * Parses CLI args and applies KOZEN_ETL_* environment variable fallbacks.
   */
  async fill(args: string[] | IArgs): Promise<IArgs> {
    const parsed     = this.extract(args) as IArgs & Record<string, unknown>;
    const moduleType = parsed['delegateType'] as string | undefined ?? process.env['KOZEN_ETL_DELEGATE_TYPE'];

    // ── MK pipeline: MongoDB → Kafka ─────────────────────────────────────────
    parsed['mk'] = parsed['mk'] ?? {};
    const mk    = parsed['mk'] as Record<string, unknown>;

    mk['source'] = mk['source'] ?? {};
    const mkSrc  = mk['source'] as Record<string, unknown>;
    mkSrc['uri']        = mkSrc['uri']        ?? parsed['mk.source.uri']        ?? process.env['KOZEN_ETL_MK_SOURCE_URI'];
    mkSrc['database']   = mkSrc['database']   ?? parsed['mk.source.database']   ?? process.env['KOZEN_ETL_MK_SOURCE_DATABASE'];
    mkSrc['collection'] = mkSrc['collection'] ?? parsed['mk.source.collection'] ?? process.env['KOZEN_ETL_MK_SOURCE_COLLECTION'];

    mk['destination'] = mk['destination'] ?? {};
    const mkDst       = mk['destination'] as Record<string, unknown>;
    mkDst['brokers']  = mkDst['brokers']  ?? (process.env['KOZEN_ETL_MK_DESTINATION_BROKERS']?.split(','));
    mkDst['topic']    = mkDst['topic']    ?? parsed['mk.destination.topic']    ?? process.env['KOZEN_ETL_MK_DESTINATION_TOPIC'];
    mkDst['clientId'] = mkDst['clientId'] ?? parsed['mk.destination.clientId'] ?? process.env['KOZEN_ETL_MK_DESTINATION_CLIENT_ID'] ?? 'etl-mk';
    mkDst['ssl']      = mkDst['ssl']      ?? (process.env['KOZEN_ETL_MK_DESTINATION_SSL'] === 'true');

    const mkFile = parsed['mk.delegateFile'] as string | undefined ?? process.env['KOZEN_ETL_MK_DELEGATE_FILE'];
    const mkKey  = parsed['mk.delegateKey']  as string | undefined ?? process.env['KOZEN_ETL_MK_DELEGATE_KEY'] ?? 'etl-mk:delegate:source';
    if (mkFile) mk['delegate'] = { key: mkKey, file: mkFile, type: 'instance', moduleType };

    mk['dlqTopic'] = mk['dlqTopic'] ?? parsed['mk.dlqTopic'] ?? process.env['KOZEN_ETL_MK_DLQ_TOPIC'];

    // ── KM pipeline: Kafka → MongoDB ─────────────────────────────────────────
    parsed['km'] = parsed['km'] ?? {};
    const km    = parsed['km'] as Record<string, unknown>;

    km['source'] = km['source'] ?? {};
    const kmSrc  = km['source'] as Record<string, unknown>;
    kmSrc['brokers']  = kmSrc['brokers']  ?? (process.env['KOZEN_ETL_KM_SOURCE_BROKERS']?.split(','));
    kmSrc['topic']    = kmSrc['topic']    ?? parsed['km.source.topic']    ?? process.env['KOZEN_ETL_KM_SOURCE_TOPIC'];
    kmSrc['groupId']  = kmSrc['groupId']  ?? parsed['km.source.groupId']  ?? process.env['KOZEN_ETL_KM_SOURCE_GROUP_ID']  ?? 'etl-mk-group';
    kmSrc['clientId'] = kmSrc['clientId'] ?? parsed['km.source.clientId'] ?? process.env['KOZEN_ETL_KM_SOURCE_CLIENT_ID'] ?? 'etl-km';
    kmSrc['ssl']      = kmSrc['ssl']      ?? (process.env['KOZEN_ETL_KM_SOURCE_SSL'] === 'true');

    km['destination'] = km['destination'] ?? {};
    const kmDst       = km['destination'] as Record<string, unknown>;
    kmDst['uri']        = kmDst['uri']        ?? parsed['km.destination.uri']        ?? process.env['KOZEN_ETL_KM_DESTINATION_URI'];
    kmDst['database']   = kmDst['database']   ?? parsed['km.destination.database']   ?? process.env['KOZEN_ETL_KM_DESTINATION_DATABASE'];
    kmDst['collection'] = kmDst['collection'] ?? parsed['km.destination.collection'] ?? process.env['KOZEN_ETL_KM_DESTINATION_COLLECTION'];

    const kmFile = parsed['km.delegateFile'] as string | undefined ?? process.env['KOZEN_ETL_KM_DELEGATE_FILE'];
    const kmKey  = parsed['km.delegateKey']  as string | undefined ?? process.env['KOZEN_ETL_KM_DELEGATE_KEY'] ?? 'etl-mk:delegate:destination';
    if (kmFile) km['delegate'] = { key: kmKey, file: kmFile, type: 'instance', moduleType };

    km['writeMode']     = km['writeMode']     ?? parsed['km.writeMode']     ?? process.env['KOZEN_ETL_KM_DESTINATION_WRITE_MODE'] ?? 'insert';
    km['dlqTopic']      = km['dlqTopic']      ?? parsed['km.dlqTopic']      ?? process.env['KOZEN_ETL_KM_DLQ_TOPIC'];
    km['retryAttempts'] = km['retryAttempts'] ?? Number(process.env['KOZEN_ETL_KM_RETRY_ATTEMPTS'] ?? 3);
    km['retryDelayMs']  = km['retryDelayMs']  ?? Number(process.env['KOZEN_ETL_KM_RETRY_DELAY_MS']  ?? 1000);

    return parsed;
  }

  /**
   * Starts the ETL pipeline with the resolved options.
   */
  async start(args?: string[] | IArgs): Promise<{ await: boolean }> {
    const flow = this.getId(args as never);
    try {
      const filled  = await this.fill((args ?? []) as IArgs);
      const options = this.buildOptions(filled as unknown as Record<string, unknown>);
      this.logger?.info({
        flow,
        src: 'EtlMk:EtlCLIController:start',
        message: 'Starting ETL pipeline',
        data: { mk: !!options.mk?.delegate, km: !!options.km?.delegate }
      });
      return await this.srvPipeline?.start(options) ?? { await: false };
    } catch (error: unknown) {
      this.logger?.error({
        flow,
        src: 'EtlMk:EtlCLIController:start',
        message: `Failed to start ETL pipeline: ${(error as Error).message}`
      });
      return { await: false };
    }
  }

  /**
   * Validates required configuration without starting the pipeline.
   */
  async validate(args?: string[] | IArgs): Promise<void> {
    const flow   = this.getId(args as never);
    const filled = await this.fill((args ?? []) as IArgs);
    const missing: string[] = [];

    const mk    = filled['mk'] as Record<string, unknown> | undefined;
    const mkSrc = mk?.['source'] as Record<string, unknown> | undefined;
    const mkDst = mk?.['destination'] as Record<string, unknown> | undefined;
    const km    = filled['km'] as Record<string, unknown> | undefined;
    const kmSrc = km?.['source'] as Record<string, unknown> | undefined;
    const kmDst = km?.['destination'] as Record<string, unknown> | undefined;

    if (!mk?.['delegate'] && !km?.['delegate']) {
      missing.push('KOZEN_ETL_MK_DELEGATE_FILE or KOZEN_ETL_KM_DELEGATE_FILE');
    }

    if (mk?.['delegate']) {
      if (!mkSrc?.['uri'])        missing.push('KOZEN_ETL_MK_SOURCE_URI');
      if (!mkSrc?.['database'])   missing.push('KOZEN_ETL_MK_SOURCE_DATABASE');
      if (!mkSrc?.['collection']) missing.push('KOZEN_ETL_MK_SOURCE_COLLECTION');
      if (!mkDst?.['brokers'])    missing.push('KOZEN_ETL_MK_DESTINATION_BROKERS');
      if (!mkDst?.['topic'])      missing.push('KOZEN_ETL_MK_DESTINATION_TOPIC');
    }

    if (km?.['delegate']) {
      if (!kmSrc?.['brokers'])    missing.push('KOZEN_ETL_KM_SOURCE_BROKERS');
      if (!kmSrc?.['topic'])      missing.push('KOZEN_ETL_KM_SOURCE_TOPIC');
      if (!kmDst?.['uri'])        missing.push('KOZEN_ETL_KM_DESTINATION_URI');
      if (!kmDst?.['database'])   missing.push('KOZEN_ETL_KM_DESTINATION_DATABASE');
      if (!kmDst?.['collection']) missing.push('KOZEN_ETL_KM_DESTINATION_COLLECTION');
    }

    if (missing.length > 0) {
      this.logger?.error({ flow, src: 'EtlMk:EtlCLIController:validate', message: 'Configuration incomplete', data: { missing } });
      throw new Error(`Missing required configuration: ${missing.join(', ')}`);
    }

    this.logger?.info({ flow, src: 'EtlMk:EtlCLIController:validate', message: 'Configuration valid',
      data: { mk: !!mk?.['delegate'], km: !!km?.['delegate'] } });
  }

  private buildOptions(filled: Record<string, unknown>): IEtlOptions {
    const mk = filled['mk'] as Record<string, unknown>;
    const km = filled['km'] as Record<string, unknown>;

    return {
      mk: mk?.['delegate'] ? mk as unknown as IMongoToKafkaConfig : undefined,
      km: km?.['delegate'] ? km as unknown as IKafkaToMongoConfig : undefined
    };
  }
}
