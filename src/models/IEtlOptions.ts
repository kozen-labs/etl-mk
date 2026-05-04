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

export interface IEtlOptions {
  flow?: string;
  mongo: IMongoConfig;
  kafka: IKafkaConfig;
  /** If set, starts the MongoDB → Kafka pipeline using this delegate. */
  sourceDelegate?: IDependency;
  /** If set, starts the Kafka → MongoDB pipeline using this delegate. */
  destinationDelegate?: IDependency;
  writeMode?: 'insert' | 'upsert';
  dlqTopic?: string;
  retryAttempts?: number;
  retryDelayMs?: number;
}
