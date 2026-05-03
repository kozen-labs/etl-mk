export type EtlMode = 'mongo-to-kafka' | 'kafka-to-mongo';

export interface IEtlSourceMongo {
  uri: string;
  database: string;
  collection: string;
}

export interface IEtlSourceKafka {
  brokers: string[];
  topic: string;
  groupId?: string;
  clientId?: string;
  ssl?: boolean;
}

export interface IEtlDestinationKafka {
  brokers: string[];
  topic: string;
  clientId?: string;
  dlqTopic?: string;
  ssl?: boolean;
}

export interface IEtlDestinationMongo {
  uri: string;
  database: string;
  collection: string;
  writeMode?: 'insert' | 'upsert';
  dlqCollection?: string;
}

export interface IEtlOptions {
  mode: EtlMode;
  delegateFile?: string;
  delegateType?: string;
  retryAttempts?: number;
  retryDelayMs?: number;
  source: IEtlSourceMongo | IEtlSourceKafka;
  destination: IEtlDestinationKafka | IEtlDestinationMongo;
}
