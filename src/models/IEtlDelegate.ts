import type { ChangeStreamDocument, Document } from 'mongodb';
import type { IEtlMongoToKafkaTools, IEtlKafkaToMongoTools } from './IEtlTools';

export type MongoToKafkaHandler = (
  change: ChangeStreamDocument<Document>,
  tools: IEtlMongoToKafkaTools
) => Promise<unknown>;

export type KafkaToMongoHandler = (
  message: unknown,
  tools: IEtlKafkaToMongoTools
) => Promise<unknown>;

export interface IEtlMongoToKafkaDelegate {
  insert?:     MongoToKafkaHandler;
  update?:     MongoToKafkaHandler;
  replace?:    MongoToKafkaHandler;
  delete?:     MongoToKafkaHandler;
  invalidate?: MongoToKafkaHandler;
  default?:    MongoToKafkaHandler;
  on?:         MongoToKafkaHandler;
}

export interface IEtlKafkaToMongoDelegate {
  message?: KafkaToMongoHandler;
  insert?:  KafkaToMongoHandler;
  on?:      KafkaToMongoHandler;
  default?: KafkaToMongoHandler;
}
