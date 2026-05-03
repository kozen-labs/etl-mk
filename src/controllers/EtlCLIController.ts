import { CLIController, IArgs } from '@kozen/engine';
import type { EtlPipelineService } from '../services/EtlPipelineService';
import type { IEtlOptions, IEtlSourceMongo, IEtlSourceKafka, IEtlDestinationKafka, IEtlDestinationMongo } from '../models/IEtlOptions';

export class EtlCLIController extends CLIController {
  private srvPipeline?: EtlPipelineService;

  constructor(dependency?: Record<string, unknown>) {
    super(dependency as never);
    this.srvPipeline = dependency?.['srvPipeline'] as EtlPipelineService;
  }

  async help(): Promise<void> {
    const mod = await this.assistant?.get('module:@kozen/etl-mk');
    super.help({
      title: `'${(mod as Record<string, Record<string, string>>)?.['metadata']?.['alias'] ?? 'etl'}' — etl-mk`,
      body: `
Bi-directional MongoDB ↔ Kafka ETL pipeline.

Usage:
  kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env

Modes:
  ETL_MODE=mongo-to-kafka   Watch a MongoDB collection → publish to Kafka topic
  ETL_MODE=kafka-to-mongo   Consume a Kafka topic      → write to MongoDB collection

Source (mongo-to-kafka):
  ETL_SOURCE_URI            MongoDB connection string
  ETL_SOURCE_DATABASE       Source database name
  ETL_SOURCE_COLLECTION     Source collection to watch

Destination (mongo-to-kafka):
  ETL_DESTINATION_BROKERS   Comma-separated Kafka brokers
  ETL_DESTINATION_TOPIC     Target Kafka topic
  ETL_DESTINATION_CLIENT_ID Kafka client ID (default: etl-mk)
  ETL_DESTINATION_DLQ_TOPIC Dead-letter topic (default: <topic>-dlq)

Source (kafka-to-mongo):
  ETL_SOURCE_BROKERS        Comma-separated Kafka brokers
  ETL_SOURCE_TOPIC          Kafka topic to consume
  ETL_SOURCE_GROUP_ID       Consumer group ID (default: etl-mk-group)

Destination (kafka-to-mongo):
  ETL_DESTINATION_URI        MongoDB connection string
  ETL_DESTINATION_DATABASE   Target database name
  ETL_DESTINATION_COLLECTION Target collection
  ETL_DESTINATION_WRITE_MODE insert | upsert (default: insert)
  ETL_DESTINATION_DLQ_COLLECTION Dead-letter collection (default: <collection>_dlq)

Delegate:
  ETL_DELEGATE_FILE         Absolute path to delegate file (omit for passthrough)
  ETL_DELEGATE_TYPE         esm | cjs (auto-detected from extension)

Logging:
  KOZEN_LOG_LEVEL           DEBUG | INFO | WARN | ERROR (default: INFO)
  KOZEN_LOG_TYPE            object | json (default: object)
      `
    });
  }

  async fill(args: string[] | IArgs): Promise<IArgs> {
    const parsed = this.extract(args) as IArgs & Record<string, unknown>;

    parsed['mode'] = parsed['mode'] ?? process.env['ETL_MODE'];

    parsed['file']         = parsed['file']         ?? process.env['ETL_DELEGATE_FILE'];
    parsed['delegateType'] = parsed['delegateType'] ?? process.env['ETL_DELEGATE_TYPE'];

    // source.*
    parsed['source'] = parsed['source'] ?? {};
    const src = parsed['source'] as Record<string, unknown>;

    src['uri']        = src['uri']        ?? parsed['source.uri']        ?? process.env['ETL_SOURCE_URI'];
    src['database']   = src['database']   ?? parsed['source.database']   ?? process.env['ETL_SOURCE_DATABASE'];
    src['collection'] = src['collection'] ?? parsed['source.collection'] ?? process.env['ETL_SOURCE_COLLECTION'];
    src['brokers']    = src['brokers']    ?? (process.env['ETL_SOURCE_BROKERS']?.split(','));
    src['topic']      = src['topic']      ?? parsed['source.topic']      ?? process.env['ETL_SOURCE_TOPIC'];
    src['groupId']    = src['groupId']    ?? parsed['source.groupId']    ?? process.env['ETL_SOURCE_GROUP_ID']    ?? 'etl-mk-group';
    src['clientId']   = src['clientId']   ?? parsed['source.clientId']   ?? process.env['ETL_SOURCE_CLIENT_ID']   ?? 'etl-mk';

    // destination.*
    parsed['destination'] = parsed['destination'] ?? {};
    const dst = parsed['destination'] as Record<string, unknown>;

    dst['uri']           = dst['uri']           ?? parsed['destination.uri']           ?? process.env['ETL_DESTINATION_URI'];
    dst['database']      = dst['database']      ?? parsed['destination.database']      ?? process.env['ETL_DESTINATION_DATABASE'];
    dst['collection']    = dst['collection']    ?? parsed['destination.collection']    ?? process.env['ETL_DESTINATION_COLLECTION'];
    dst['brokers']       = dst['brokers']       ?? (process.env['ETL_DESTINATION_BROKERS']?.split(','));
    dst['topic']         = dst['topic']         ?? parsed['destination.topic']         ?? process.env['ETL_DESTINATION_TOPIC'];
    dst['clientId']      = dst['clientId']      ?? parsed['destination.clientId']      ?? process.env['ETL_DESTINATION_CLIENT_ID']      ?? 'etl-mk';
    dst['dlqTopic']      = dst['dlqTopic']      ?? parsed['destination.dlqTopic']      ?? process.env['ETL_DESTINATION_DLQ_TOPIC'];
    dst['writeMode']     = dst['writeMode']     ?? parsed['destination.writeMode']     ?? process.env['ETL_DESTINATION_WRITE_MODE']     ?? 'insert';
    dst['dlqCollection'] = dst['dlqCollection'] ?? parsed['destination.dlqCollection'] ?? process.env['ETL_DESTINATION_DLQ_COLLECTION'];

    return parsed;
  }

  async start(args?: string[] | IArgs): Promise<{ await: boolean }> {
    const flow = this.getId(args as never);

    try {
      const filled = await this.fill((args ?? []) as IArgs);
      const options = this.buildOptions(filled as unknown as Record<string, unknown>);

      this.logger?.info({
        flow,
        src: 'EtlMk:CLI:start',
        message: `Starting ETL pipeline`,
        data: { mode: options.mode }
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
    const flow = this.getId(args as never);
    const filled = await this.fill((args ?? []) as IArgs);

    const missing: string[] = [];
    const mode = filled['mode'] as string;

    if (!mode) missing.push('ETL_MODE');

    if (mode === 'mongo-to-kafka') {
      const src = filled['source'] as Record<string, unknown>;
      const dst = filled['destination'] as Record<string, unknown>;
      if (!src['uri'])        missing.push('ETL_SOURCE_URI');
      if (!src['database'])   missing.push('ETL_SOURCE_DATABASE');
      if (!src['collection']) missing.push('ETL_SOURCE_COLLECTION');
      if (!dst['brokers'])    missing.push('ETL_DESTINATION_BROKERS');
      if (!dst['topic'])      missing.push('ETL_DESTINATION_TOPIC');
    }

    if (mode === 'kafka-to-mongo') {
      const src = filled['source'] as Record<string, unknown>;
      const dst = filled['destination'] as Record<string, unknown>;
      if (!src['brokers']) missing.push('ETL_SOURCE_BROKERS');
      if (!src['topic'])   missing.push('ETL_SOURCE_TOPIC');
      if (!dst['uri'])        missing.push('ETL_DESTINATION_URI');
      if (!dst['database'])   missing.push('ETL_DESTINATION_DATABASE');
      if (!dst['collection']) missing.push('ETL_DESTINATION_COLLECTION');
    }

    if (missing.length > 0) {
      this.logger?.error({
        flow,
        src: 'EtlMk:CLI:validate',
        message: `Configuration incomplete`,
        data: { missing }
      });
      throw new Error(`Missing required configuration: ${missing.join(', ')}`);
    }

    this.logger?.info({
      flow,
      src: 'EtlMk:CLI:validate',
      message: `Configuration valid`,
      data: { mode }
    });
  }

  private buildOptions(filled: Record<string, unknown>): IEtlOptions {
    const mode = filled['mode'] as IEtlOptions['mode'];

    const source = mode === 'mongo-to-kafka'
      ? filled['source'] as IEtlSourceMongo
      : { ...(filled['source'] as IEtlSourceKafka) };

    const destination = mode === 'mongo-to-kafka'
      ? filled['destination'] as IEtlDestinationKafka
      : filled['destination'] as IEtlDestinationMongo;

    return {
      mode,
      delegateFile: filled['file']         as string | undefined,
      delegateType: filled['delegateType'] as string | undefined,
      source,
      destination
    };
  }
}
