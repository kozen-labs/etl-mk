import type { ITriggerTools } from '@kozen/trigger';
import type { Db, Collection } from 'mongodb';
import type { IIoC } from '@kozen/engine';

export interface IEtlMongoToKafkaTools extends ITriggerTools {
  mode: 'mongo-to-kafka';
  setMessageKey(key: string): void;
  setMessageHeaders(headers: Record<string, string>): void;
}

export interface IEtlKafkaToMongoTools {
  mode: 'kafka-to-mongo';
  flow: string;
  db?: Db;
  collection?: Collection;
  dbName?: string;
  collectionName?: string;
  assistant?: IIoC;
}
