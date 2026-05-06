import type { ITriggerTools } from '@kozen/trigger';

/**
 * Delegate interface for the Kafka → MongoDB pipeline.
 * Handlers receive the parsed Kafka message and return the document to write.
 * Return null or undefined to skip the write.
 */
export interface IKafkaDelegate {
  message?: (msg: unknown, tools?: ITriggerTools) => Promise<unknown> | unknown;
  on?:      (msg: unknown, tools?: ITriggerTools) => Promise<unknown> | unknown;
  default?: (msg: unknown, tools?: ITriggerTools) => Promise<unknown> | unknown;
}
