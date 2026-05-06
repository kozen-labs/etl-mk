# @kozen/etl-mk

Bi-directional MongoDB ↔ Kafka ETL pipeline module for the [Kozen](https://github.com/kozen-labs) framework.

Define one delegate file to run a single pipeline direction, or define both for a full bidirectional pipeline. Each direction is an independent process controlled by its own configuration.

---

## How it works

```
MongoDB change stream ──► MK delegate ──► Kafka topic
Kafka topic ──────────► KM delegate ──► MongoDB collection
```

A **delegate** is a plain `.mjs` or `.cjs` file that exports handler functions. The pipeline loads the delegate at startup and calls the appropriate handler for each event. Handlers return the transformed payload, or `null`/`undefined` to skip.

The MK direction uses `@kozen/trigger`'s `ChangeStreamService` under the hood. The KM direction implements at-least-once delivery with configurable retry and dead-letter routing.

---

## Install

```bash
npm install @kozen/etl-mk
```

Requires Node.js 18 or later. `kafkajs` is bundled as a runtime dependency. A Kafka cluster and a MongoDB replica set or Atlas cluster are required at runtime.

---

## Quick reference

```bash
# Start (reads KOZEN_ETL_* from environment or --envFile)
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env

# Validate configuration without connecting to any service
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:validate --envFile=.env

# Print full help with all flags and environment variables
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:help
```

---

## Quick start

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
KOZEN_ETL_MK_SOURCE_URI=mongodb+srv://user:pass@cluster.mongodb.net/ \
KOZEN_ETL_MK_SOURCE_DATABASE=mydb \
KOZEN_ETL_MK_SOURCE_COLLECTION=orders \
KOZEN_ETL_MK_DESTINATION_BROKERS=broker1:9092 \
KOZEN_ETL_MK_DESTINATION_TOPIC=orders.events \
KOZEN_ETL_MK_DELEGATE_FILE=/app/delegates/orders.mjs \
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start
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
KOZEN_ETL_KM_DESTINATION_URI=mongodb+srv://user:pass@cluster.mongodb.net/ \
KOZEN_ETL_KM_DESTINATION_DATABASE=mydb \
KOZEN_ETL_KM_DESTINATION_COLLECTION=orders_archive \
KOZEN_ETL_KM_DELEGATE_FILE=/app/delegates/archive.mjs \
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start
```

### Bidirectional pipeline

Configure both `KOZEN_ETL_MK_*` and `KOZEN_ETL_KM_*` in the same `.env` file. Copy a ready-made template from the package:

```bash
cp node_modules/@kozen/etl-mk/cfg/env.bidirectional.example .env
# fill in connection strings and delegate paths
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env
```

---

## Documentation

| Page | Description |
|---|---|
| [Get Started](https://github.com/kozen-labs/etl-mk/wiki/Get-Started) | Installation and minimal working examples |
| [Configuration](https://github.com/kozen-labs/etl-mk/wiki/Configuration) | Full `KOZEN_ETL_*` variable reference and `.env` templates |
| [ETL via CLI](https://github.com/kozen-labs/etl-mk/wiki/ETL-via-CLI) | CLI actions, flags, and examples |
| [Delegate](https://github.com/kozen-labs/etl-mk/wiki/Delegate) | Writing MK and KM delegate handlers; error handling and DLQ |
| [API](https://github.com/kozen-labs/etl-mk/wiki/API) | Programmatic SDK — types and service classes |
| [Kozen Integration](https://github.com/kozen-labs/etl-mk/wiki/Kozen-Integration) | IoC tokens, module composition, delegate loading internals |
| [Contributing Policy](https://github.com/kozen-labs/etl-mk/wiki/POLICY) | Branch model, PR requirements, code standards |

---

## Deployment

### PM2

```javascript
// ecosystem.config.js
module.exports = {
  apps: [{
    name:   'orders-etl',
    script: 'node_modules/@kozen/engine/dist/bin/kozen.js',
    args:   '--moduleLoad=@kozen/etl-mk --action=etl:start',
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
CMD ["npx", "kozen", "--moduleLoad=@kozen/etl-mk", "--action=etl:start"]
```

```bash
docker run -d \
  -e KOZEN_ETL_MK_SOURCE_URI="mongodb+srv://..." \
  -e KOZEN_ETL_MK_SOURCE_DATABASE=production \
  -e KOZEN_ETL_MK_SOURCE_COLLECTION=orders \
  -e KOZEN_ETL_MK_DESTINATION_BROKERS="broker1:9092" \
  -e KOZEN_ETL_MK_DESTINATION_TOPIC=orders.events \
  -e KOZEN_ETL_MK_DELEGATE_FILE=/app/delegates/orders.mjs \
  -v /host/delegates:/app/delegates \
  my-etl-mk-image
```

---

## Development

```bash
npm install
npx tsc --noEmit                              # type-check
npm run build                                 # compile + copy assets to dist/
npm run dev -- --action=etl:help              # run with ts-node
npm run dev -- --action=etl:start --envFile=cfg/env.bidirectional.example
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:validate --envFile=.env
```

The module entry point is [src/index.ts](src/index.ts). The compiled output is written to `dist/`.

---

## Related

- [`@kozen/engine`](https://github.com/kozen-labs/engine) — Kozen Task Execution Framework
- [`@kozen/trigger`](https://github.com/kozen-labs/trigger/wiki) — self-hosted MongoDB change stream triggers
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [kafkajs](https://github.com/tulios/kafkajs) — Kafka client for Node.js
