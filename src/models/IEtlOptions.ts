import type { IDependency } from '@kozen/engine';

export interface IMongoConfig {
  uri: string;
  database: string;
  collection: string;
}

export interface IKafkaConfig {
  brokers: string[];
  topic: string;
  groupId?: string;
  clientId?: string;
  ssl?: boolean;
}

/**
 * Configuration for the MongoDB → Kafka pipeline.
 */
export interface IMongoToKafkaConfig {
  source:      IMongoConfig;
  destination: IKafkaConfig;
  /**
   * Presence of this field enables the pipeline. Absence disables it.
   */
  delegate?:   IDependency;
  dlqTopic?:   string;
}

/**
 * Configuration for the Kafka → MongoDB pipeline.
 */
export interface IKafkaToMongoConfig {
  source:        IKafkaConfig;
  destination:   IMongoConfig;
  /**
   * Presence of this field enables the pipeline. Absence disables it.
   */
  delegate?:     IDependency;
  writeMode?:    'insert' | 'upsert';
  dlqTopic?:     string;
  retryAttempts?: number;
  retryDelayMs?:  number;
}

export interface IEtlOptions {
  flow?: string;
  /**
   * MongoDB → Kafka pipeline. Omit to disable this direction.
   */
  mk?: IMongoToKafkaConfig;
  /**
   * Kafka → MongoDB pipeline. Omit to disable this direction.
   */
  km?: IKafkaToMongoConfig;
}
