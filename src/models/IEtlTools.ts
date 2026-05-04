import type { ITriggerTools } from '@kozen/trigger';

/** Tools passed to mongo-to-kafka delegate handlers. Extends ITriggerTools. */
export interface IEtlMongoToKafkaTools extends ITriggerTools {
  setMessageKey(key: string): void;
  setMessageHeaders(headers: Record<string, string>): void;
}
