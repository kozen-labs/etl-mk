# EP: etl-mk — Bi-directional MongoDB ↔ Kafka ETL Pipeline Module

## Status
Draft  
Version: 1.0 | Author: Antonio Membrides Espinosa | Last updated: 2026-05-03  
PRD reference: `doc/requirements/etl-mk.prd.md`

---

## 1. Background

`etl-mk` (`@kozen/etl-mk`) is a Kozen module that wraps `@kozen/trigger` and `kafkajs`
into a zero-boilerplate, bi-directional ETL pipeline. It eliminates the repeated scaffolding
engineers write when connecting MongoDB change streams to Kafka topics (and vice versa),
while following the same delegate pattern and IoC conventions already established by
`@kozen/trigger`. Full business context is in the PRD.

---

## 2. Goals and non-goals (engineering)

**Goals:**
- Implement a fully functional `mongo-to-kafka` pipeline (change stream → Kafka producer)
- Implement a fully functional `kafka-to-mongo` pipeline (Kafka consumer → MongoDB write)
- Reuse `ChangeStreamService` from `@kozen/trigger` — no reimplementation
- Follow the Kozen module structure: `KzModule`, `KzController`, `BaseService`, IoC JSON configs
- Publish as a proper npm package with TypeScript declarations (`dist/`)
- All configuration dual-accessible: env var **and** CLI flag, with safe defaults
- Zero `console.log` calls — all output via Kozen `logger:service`
- Dead-letter routing for both modes (DLQ topic / collection)
- Built-in passthrough delegate (no user code required)

**Non-goals (v1):**
- Schema Registry / Avro serialization
- Exactly-once semantics
- Multi-topic fan-out
- Prometheus metrics endpoint or OpenTelemetry spans
- Web UI, REST API, or MCP controller
- Message ordering guarantees beyond Kafka + MongoDB native behaviour

---

## 3. Proposed solution

### 3.1 Overview

```
mongo-to-kafka:
  MongoDB collection
      │ ChangeStreamService (@kozen/trigger)
      ↓
  DelegateLoaderService.dispatch(change, tools)   ← user delegate or passthrough
      │ return value (null → skip)
      ↓
  KafkaProducerService.publish(topic, payload)
      │ error
      ↓
  KafkaProducerService.publishDLQ(dlqTopic, { originalPayload, error, flow })

kafka-to-mongo:
  Kafka topic
      │ KafkaConsumerService.subscribe(topic, groupId)
      ↓
  DelegateLoaderService.dispatch(message, tools)  ← user delegate or passthrough
      │ return value (null → skip)
      ↓
  MongoWriterService.write(collection, document)
      │ error
      ↓
  MongoWriterService.writeDLQ(dlqCollection, { originalMessage, error, flow })
```

### 3.2 Architecture and system design

The module follows the Kozen module pattern exactly (see `@kozen/trigger` and
`@kozen/engine` as reference implementations). It adds **no new architectural concepts**
— it extends the existing pattern with two new pipeline services.

```
etl-mk/
├── src/
│   ├── index.ts                         ← EtlModule (KzModule subclass)
│   ├── controllers/
│   │   └── EtlCLIController.ts          ← etl:start, etl:help, etl:validate
│   ├── services/
│   │   ├── EtlPipelineService.ts        ← reads ETL_MODE, delegates to the right service
│   │   ├── MongoToKafkaService.ts       ← ChangeStreamService + delegate + Kafka producer
│   │   ├── KafkaToMongoService.ts       ← Kafka consumer + delegate + MongoWriter
│   │   ├── KafkaProducerService.ts      ← kafkajs Kafka producer wrapper + DLQ
│   │   ├── KafkaConsumerService.ts      ← kafkajs Kafka consumer wrapper
│   │   ├── MongoWriterService.ts        ← mongodb insertOne / updateOne (upsert)
│   │   └── DelegateLoaderService.ts     ← ESM/CJS dynamic import; passthrough fallback
│   ├── models/
│   │   ├── IEtlOptions.ts               ← full configuration interface
│   │   ├── IEtlTools.ts                 ← tools injected into delegate handlers
│   │   └── IEtlDelegate.ts              ← delegate function type signatures
│   └── configs/
│       ├── ioc.json                     ← always loaded: core services
│       └── cli.json                     ← additional registrations for CLI runtime
├── cfg/
│   └── config.json                      ← standalone run config
├── package.json
└── tsconfig.json
```

**Dependency graph** (all references are unidirectional; no cycles):

```
EtlModule
  └── EtlCLIController
        └── EtlPipelineService
              ├── MongoToKafkaService
              │     ├── ChangeStreamService  (@kozen/trigger — reused as-is)
              │     ├── DelegateLoaderService
              │     └── KafkaProducerService
              └── KafkaToMongoService
                    ├── KafkaConsumerService
                    ├── DelegateLoaderService
                    └── MongoWriterService
```

`DelegateLoaderService` is shared: it handles both delegate loading strategies (ESM/CJS
dynamic import, module-system auto-detection) and returns the built-in passthrough if no
`ETL_DELEGATE_FILE` is configured.

### 3.3 Data model

No persistent data model changes for v1.0. All state is in-flight.

**Configuration interface (IEtlOptions.ts)**:

```typescript
export interface IEtlSourceMongo {
  uri: string;
  database: string;
  collection: string;
}

export interface IEtlSourceKafka {
  brokers: string[];         // parsed from comma-separated ETL_SOURCE_BROKERS
  topic: string;
  groupId?: string;          // default: 'etl-mk-group'
  clientId?: string;         // default: 'etl-mk'
  ssl?: boolean;             // default: false
}

export interface IEtlDestinationKafka {
  brokers: string[];
  topic: string;
  clientId?: string;         // default: 'etl-mk'
  dlqTopic?: string;         // default: '<topic>-dlq'
  ssl?: boolean;
}

export interface IEtlDestinationMongo {
  uri: string;
  database: string;
  collection: string;
  writeMode?: 'insert' | 'upsert';   // default: 'insert'
  dlqCollection?: string;            // default: '<collection>_dlq'
}

export type EtlMode = 'mongo-to-kafka' | 'kafka-to-mongo';

export interface IEtlOptions {
  mode: EtlMode;
  delegateFile?: string;
  delegateType?: string;        // 'esm' | 'cjs' — auto-detected if omitted
  retryAttempts?: number;       // default: 3
  retryDelayMs?: number;        // default: 1000
  source:  IEtlSourceMongo  | IEtlSourceKafka;
  destination: IEtlDestinationKafka | IEtlDestinationMongo;
}
```

**Tools injected into delegate handlers (IEtlTools.ts)**:

```typescript
import type { ITriggerTools } from '@kozen/trigger';
import type { Db, Collection } from 'mongodb';
import type { IIoC } from '@kozen/engine';

// Extends ITriggerTools — same shape as @kozen/trigger; additive only
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
```

**Delegate signatures (IEtlDelegate.ts)**:

```typescript
// mongo-to-kafka: same event names as @kozen/trigger
export type MongoToKafkaHandler = (
  change: any,
  tools: IEtlMongoToKafkaTools
) => Promise<any | null>;

// kafka-to-mongo: handler names 'message', 'insert', 'on', or 'default'
export type KafkaToMongoHandler = (
  message: any,
  tools: IEtlKafkaToMongoTools
) => Promise<any | null>;

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
```

### 3.4 API design — CLI actions and environment variables

**CLI actions:**

```bash
# Start a pipeline (reads from .env or inline flags)
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env

# Show help
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:help

# Validate config and connectivity without starting
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:validate --envFile=.env
```

**Environment variable → CLI flag mapping** (see PRD §3.3 for the full table):

| Env Variable | CLI Flag | Default |
|---|---|---|
| `ETL_MODE` | `--mode` | — (required) |
| `ETL_DELEGATE_FILE` | `--file` | — (passthrough if omitted) |
| `ETL_SOURCE_URI` | `--source.uri` | — |
| `ETL_SOURCE_DATABASE` | `--source.database` | — |
| `ETL_SOURCE_COLLECTION` | `--source.collection` | — |
| `ETL_SOURCE_BROKERS` | `--source.brokers` | — |
| `ETL_SOURCE_TOPIC` | `--source.topic` | — |
| `ETL_SOURCE_GROUP_ID` | `--source.groupId` | `etl-mk-group` |
| `ETL_DESTINATION_BROKERS` | `--destination.brokers` | — |
| `ETL_DESTINATION_TOPIC` | `--destination.topic` | — |
| `ETL_DESTINATION_DLQ_TOPIC` | `--destination.dlqTopic` | `<topic>-dlq` |
| `ETL_DESTINATION_URI` | `--destination.uri` | — |
| `ETL_DESTINATION_DATABASE` | `--destination.database` | — |
| `ETL_DESTINATION_COLLECTION` | `--destination.collection` | — |
| `ETL_DESTINATION_WRITE_MODE` | `--destination.writeMode` | `insert` |
| `ETL_DESTINATION_DLQ_COLLECTION` | `--destination.dlqCollection` | `<collection>_dlq` |
| `ETL_RETRY_ATTEMPTS` | `--retryAttempts` | `3` |
| `ETL_RETRY_DELAY_MS` | `--retryDelayMs` | `1000` |
| `KOZEN_LOG_LEVEL` | — | `INFO` |
| `KOZEN_LOG_TYPE` | — | `object` |

### 3.5 Security considerations

- Connection strings via env vars or `@kozen/secret` — never embedded in delegate or
  module source code.
- Full document payloads logged only at `DEBUG` level to prevent PII leakage at `INFO`.
- Kafka SSL configurable via `ETL_SOURCE_SSL` / `ETL_DESTINATION_SSL`; defaults to `false`
  (enforce `true` in production deployments via deployment config).
- MongoDB connections must use `mongodb+srv://` or explicit `tls=true` in URI.
- The `ETL_DELEGATE_FILE` path is loaded via Node.js dynamic `import()` / `require()`; the
  file must be on a trusted, non-writable path. No user-provided code evaluation beyond
  loading a static file path.
- All log entries strip credential fields before output (`uri`, `password`, `sasl.*`).

### 3.6 Testing strategy

- **Unit tests** — each service in isolation with mocked dependencies:
  - `DelegateLoaderService`: ESM load, CJS load, passthrough fallback, missing-file error
  - `KafkaProducerService`: publish success, DLQ routing on error
  - `KafkaConsumerService`: message dispatch, offset commit behaviour
  - `MongoWriterService`: insertOne, upsert, DLQ collection write
  - `EtlPipelineService`: mode routing (mongo-to-kafka vs kafka-to-mongo)
- **Integration tests** — require real MongoDB + Kafka (local Docker Compose):
  - Full `mongo-to-kafka` flow: insert a document → verify Kafka message arrives
  - Full `kafka-to-mongo` flow: produce a Kafka message → verify MongoDB document inserted
  - DLQ routing: delegate throws → message appears in DLQ sink
  - Passthrough: no delegate file → raw payload forwarded unchanged
- **Contract tests** — verify delegate interface compatibility with `@kozen/trigger`
  `ITriggerTools` so the extension is additive only

---

## 4. Implementation phases

| Phase | Scope | Dependency | Est. |
|---|---|---|---|
| 0 | TypeScript scaffold: `tsconfig.json`, updated `package.json`, `src/` skeleton, IoC JSON configs | None | 0.5d |
| 1 | Models: `IEtlOptions`, `IEtlTools`, `IEtlDelegate` | Phase 0 | 0.5d |
| 2 | `DelegateLoaderService` (ESM/CJS + passthrough) | Phase 1 | 1d |
| 3 | `KafkaProducerService` + `KafkaConsumerService` | Phase 0 | 1d |
| 4 | `MongoWriterService` | Phase 0 | 0.5d |
| 5 | `MongoToKafkaService` (wires Phase 2, 3, DLQ) | Phases 2–4 | 1d |
| 6 | `KafkaToMongoService` (wires Phase 2, 3, 4, DLQ) | Phases 2–4 | 1d |
| 7 | `EtlPipelineService` + `EtlCLIController` + `EtlModule` | Phases 5–6 | 1d |
| 8 | Unit tests (all services) | Phase 7 | 1d |
| 9 | Integration tests (Docker Compose fixture) + `etl:validate` action | Phase 8 | 1d |
| **Total** | | | **~7.5d** |

Each phase produces a separately reviewable commit. Phases 3 and 4 can run in parallel.

---

## 5. Migration plan

Greenfield — no existing users or data. No migration required.

The current `package.json` entry point (`"main": "index.js"`) will be updated to
`"main": "dist/index.js"` and `"types": "dist/index.d.ts"` as part of Phase 0.

---

## 6. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| `kafkajs` API changes between versions cause runtime errors | Medium | High | Pin supported range in `peerDependencies`; test against kafkajs `^2.x` |
| Change stream cursor lost on process restart → event gap | Medium | High | v1.1: persist resume token to `_etl_checkpoint` MongoDB collection |
| `@kozen/trigger` `ITriggerTools` interface changes in a minor version | Low | Medium | Extend, never modify `ITriggerTools`; run contract test on CI |
| Delegate dynamic `import()` fails silently on Windows path separators | Low | Medium | Normalise paths in `DelegateLoaderService` with `pathToFileURL()` before import |
| Kafka consumer rebalance under high lag causes duplicate processing | Low | Medium | Document consumer tuning; DLQ handles duplicates gracefully |
| PII logged at INFO level if delegate returns sensitive data | Low | High | Log only `{ flow, status, topic/collection, durationMs }` at INFO; full payload only at DEBUG |

---

## 7. Alternatives considered

### Alternative A — Call `ChangeStreamService.start()` directly and pass an `ITriggerDelegate`

`ChangeStreamService.start()` accepts an IoC-resolved delegate and manages the change stream
lifecycle. **Rejected:** `ChangeStreamService.onChange()` discards the handler return value
(`typeof handler === 'function' && await handler.apply(this, [change, tools])`), so it cannot
pass a payload to the Kafka producer. Extending the class and overriding `onChange()` was
considered but adds complexity without benefit: the change stream setup is only ~20 lines of
`MongoClient` code. `MongoToKafkaService` uses `MongoClient` directly and reuses the
`@kozen/trigger` `ITriggerDelegate` and `ITriggerTools` interfaces as the public contract.

### Alternative B — Expose `kafkajs` client directly in `IEtlTools` instead of wrapping it

Simpler implementation: no `KafkaProducerService` wrapper; the delegate uses the raw
`kafkajs` producer. **Rejected:** it leaks the `kafkajs` API surface into every user
delegate, making delegates depend on `kafkajs` types and preventing future swaps
(e.g., `node-rdkafka`). The wrapper surface is small (one `publish` call) and the coupling
cost of removing it is high once delegates are written against the raw client.

### Alternative C — Single merged service (`EtlService`) instead of separate `MongoToKafkaService` / `KafkaToMongoService`

Reduces the number of files. **Rejected:** the two modes have entirely different dependency
graphs (one uses a change stream + Kafka producer; the other uses a Kafka consumer + MongoDB
writer). A merged service would need conditionals throughout, violating Single Responsibility.
Two focused services are easier to test and reason about independently.

### Alternative D — `kafkajs` as a peer dependency

Forces users to install `kafkajs` separately. **Rejected for v1:** raises the barrier to
adoption and causes confusing "cannot find module" errors. Runtime dependency with a pinned
minor range (`^2.2`) is the right call at this scale. Revisit for v2 if version conflicts
become a real issue in downstream projects.

---

## 8. Open questions

| # | Question | Owner | Due |
|---|---|---|---|
| 1 | Should `etl:validate` check Kafka broker connectivity via a test producer connect, or just validate config shape? | Engineering | Phase 9 |
| 2 | Should `MongoWriterService` support bulk inserts (`insertMany`) for high-throughput kafka-to-mongo? | Engineering | Phase 6 |
| 3 | Should the resume token checkpoint collection name be configurable or always `_etl_checkpoint`? | Engineering | Phase 5 |

---

## 9. Estimates

| Phase | Scope | Estimate | Confidence |
|---|---|---|---|
| 0 | TypeScript scaffold | 0.5d | High |
| 1–4 | Models + Kafka services + MongoWriter + DelegateLoader | 3d | High |
| 5–6 | Pipeline orchestrators (MongoToKafka + KafkaToMongo) | 2d | Medium |
| 7 | CLI controller + Module entry point | 1d | High |
| 8–9 | Unit + integration tests | 2d | Medium |
| **Total** | | **~7.5 days** | |
