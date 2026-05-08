# 🚀 Kozen ETL MongoDB - Kafka: Bidirectional Pipeline Module

Bi-directional MongoDB ↔ Kafka ETL pipeline module for the [Kozen](https://github.com/kozen-labs) framework.

Define one delegate file to run a single pipeline direction, or define both for a full
bidirectional pipeline. Each direction is an independent process controlled by its own
configuration.

---

## 🌟 Key Features

- **Bi-directional in one process**: MongoDB → Kafka (MK) and Kafka → MongoDB (KM) run concurrently, each controlled by an independent delegate
- **Delegate-driven transforms**: your code handles only the business logic; the module manages connections, retries, and delivery guarantees
- **At-least-once delivery**: the KM pipeline commits Kafka offsets only after a successful write or dead-letter routing; configurable retry with exponential backoff
- **IoC-native delegate loading**: ESM (`.mjs`) and CJS (`.cjs`) delegates are resolved through the Kozen container; the same delegate can be reused across modules
- **Built on `@kozen/trigger`**: reuses `ChangeStreamService` for the MK direction; no change stream reimplementation
- **Zero-boilerplate activation**: set a delegate file variable to enable a direction; omit it to disable; no mode flags required
- **Structured logging**: all output via Kozen `logger:service`; zero `console.log`; PII-safe at `INFO` level
- **Full TypeScript declarations**: `IEtlMongoToKafkaTools extends ITriggerTools`; all public types exported from the barrel

---

## ⚡ Why Use This?

Wiring MongoDB change streams to Kafka, or consuming Kafka messages into MongoDB, requires
setting up KafkaJS producers and consumers, MongoDB cursors, offset management, retry loops,
and dead-letter routing. All of that infrastructure must exist before writing a single line
of business logic.

Kozen ETL MongoDB - Kafka handles that infrastructure layer. Provide a delegate file that
transforms each event; the module manages connections, retries, and delivery guarantees.
Because delegates receive `tools.assistant` (the Kozen IoC container), they can compose
with `@kozen/secret`, `@kozen/iam-rectification`, and any other module in the ecosystem.

---

## 📦 Installation

```bash
npm install @kozen/etl-mk
```

Requires Node.js 18 or later. `kafkajs` is bundled as a runtime dependency. A Kafka
cluster and a MongoDB replica set or Atlas cluster are required at runtime.

Quick commands:

```bash
# Start (reads KOZEN_ETL_* from environment or --envFile)
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:start --envFile=.env

# Validate configuration without connecting
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:validate --envFile=.env

# Print full help
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:help
```

---

## 📚 References

| Page | Description |
|---|---|
| [Get Started](https://github.com/kozen-labs/etl-mk/wiki/Get-Started) | Installation and minimal working examples |
| [Configuration](https://github.com/kozen-labs/etl-mk/wiki/Configuration) | Full `KOZEN_ETL_*` variable reference and `.env` templates |
| [ETL via CLI](https://github.com/kozen-labs/etl-mk/wiki/ETL-via-CLI) | CLI actions, flags, and examples |
| [Delegate](https://github.com/kozen-labs/etl-mk/wiki/Delegate) | Writing MK and KM delegate handlers; error handling and DLQ |
| [API](https://github.com/kozen-labs/etl-mk/wiki/API) | Programmatic SDK: types and service classes |
| [Kozen Integration](https://github.com/kozen-labs/etl-mk/wiki/Kozen-Integration) | IoC tokens, module composition, delegate loading internals |
| [Deployment](https://github.com/kozen-labs/etl-mk/wiki/Deployment) | Docker Compose stack: Kafka, MongoDB replica set, and ETL service, step-by-step tutorial |
| [Contributing Policy](https://github.com/kozen-labs/etl-mk/wiki/POLICY) | Licence, disclaimer, branch model, code standards |

**External resources:**

- [`@kozen/engine`](https://github.com/kozen-labs/engine/wiki): Kozen Task Execution Framework
- [`@kozen/trigger`](https://github.com/kozen-labs/trigger/wiki): self-hosted MongoDB change stream triggers
- [npm: @kozen/etl-mk](https://www.npmjs.com/package/@kozen/etl-mk): package registry
- [GitHub repository](https://github.com/kozen-labs/etl-mk): source and issues
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/): official MongoDB documentation
- [kafkajs](https://github.com/tulios/kafkajs): Kafka client for Node.js

> This project is open source and distributed under the terms described in the
> [Contributing Policy and Usage Disclaimer](https://github.com/kozen-labs/etl-mk/wiki/POLICY).

---

## 🚀 Quick start

### MongoDB → Kafka

Create a delegate that handles the change events you care about:

```javascript
// delegates/orders.mjs
export async function insert(change, tools) {
  return {
    id:     change.fullDocument._id.toString(),
    status: change.fullDocument.status
  };
}
```

Set the required variables and run:

```bash
KOZEN_ETL_MK_SOURCE_URI=mongodb+srv://appUser:secret@cluster.mongodb.net/ \
KOZEN_ETL_MK_SOURCE_DATABASE=mydb \
KOZEN_ETL_MK_SOURCE_COLLECTION=orders \
KOZEN_ETL_MK_DESTINATION_BROKERS=broker1:9092 \
KOZEN_ETL_MK_DESTINATION_TOPIC=orders.events \
KOZEN_ETL_MK_DELEGATE_FILE=/app/delegates/orders.mjs \
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:start
```

### Kafka → MongoDB

```javascript
// delegates/archive.mjs
export async function message(msg, tools) {
  return { ...msg, archivedAt: new Date() };
}
```

```bash
KOZEN_ETL_KM_SOURCE_BROKERS=broker1:9092 \
KOZEN_ETL_KM_SOURCE_TOPIC=orders.events \
KOZEN_ETL_KM_DESTINATION_URI=mongodb+srv://appUser:secret@cluster.mongodb.net/ \
KOZEN_ETL_KM_DESTINATION_DATABASE=mydb \
KOZEN_ETL_KM_DESTINATION_COLLECTION=orders_archive \
KOZEN_ETL_KM_DELEGATE_FILE=/app/delegates/archive.mjs \
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:start
```

### Bidirectional pipeline

Configure both `KOZEN_ETL_MK_*` and `KOZEN_ETL_KM_*` in the same `.env` file:

```bash
cp node_modules/@kozen/etl-mk/cfg/env.bidirectional.example .env
# fill in connection strings and delegate paths
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:start --envFile=.env
```

---

## 🔧 Deployment

### PM2

```javascript
// ecosystem.config.js
module.exports = {
  apps: [{
    name:   'orders-etl',
    script: 'node_modules/@kozen/engine/dist/bin/kozen.js',
    args:   '--moduleLoad=@kozen/etl-mk --action=etl-mk:start',
    env: {
      KOZEN_ETL_MK_SOURCE_URI:             process.env.MONGO_URI,
      KOZEN_ETL_MK_SOURCE_DATABASE:        'production',
      KOZEN_ETL_MK_SOURCE_COLLECTION:      'orders',
      KOZEN_ETL_MK_DESTINATION_BROKERS:    process.env.KAFKA_BROKERS,
      KOZEN_ETL_MK_DESTINATION_TOPIC:      'orders.events',
      KOZEN_ETL_MK_DELEGATE_FILE:          '/opt/app/delegates/orders.mjs',
      KOZEN_ETL_KM_SOURCE_BROKERS:         process.env.KAFKA_BROKERS,
      KOZEN_ETL_KM_SOURCE_TOPIC:           'orders.events',
      KOZEN_ETL_KM_DESTINATION_URI:        process.env.MONGO_URI,
      KOZEN_ETL_KM_DESTINATION_DATABASE:   'production',
      KOZEN_ETL_KM_DESTINATION_COLLECTION: 'orders_archive',
      KOZEN_ETL_KM_DELEGATE_FILE:          '/opt/app/delegates/archive.mjs',
      KOZEN_LOG_LEVEL:                     'INFO'
    },
    restart_delay: 5000,
    max_restarts:  10
  }]
};
```

### Docker

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY delegates/ ./delegates/
CMD ["npx", "kozen", "--moduleLoad=@kozen/etl-mk", "--action=etl-mk:start"]
```

```bash
docker run -d \
  -e KOZEN_ETL_MK_SOURCE_URI="mongodb+srv://appUser:secret@cluster.mongodb.net/" \
  -e KOZEN_ETL_MK_SOURCE_DATABASE=production \
  -e KOZEN_ETL_MK_SOURCE_COLLECTION=orders \
  -e KOZEN_ETL_MK_DESTINATION_BROKERS="broker1:9092" \
  -e KOZEN_ETL_MK_DESTINATION_TOPIC=orders.events \
  -e KOZEN_ETL_MK_DELEGATE_FILE=/app/delegates/orders.mjs \
  -v /host/delegates:/app/delegates \
  my-etl-mk-image
```

---

## 🛠️ Development

```bash
npm install
npx tsc --noEmit                              # type-check
npm run build                                 # compile + copy assets to dist/
npm run dev -- --action=etl-mk:help              # run with ts-node
npm run dev -- --action=etl-mk:start --envFile=cfg/env.bidirectional.example
npx kozen --moduleLoad=@kozen/etl-mk --action=etl-mk:validate --envFile=.env
```

The module entry point is [src/index.ts](src/index.ts). The compiled output is written to `dist/`.
