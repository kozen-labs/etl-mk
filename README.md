# 🔄 Kozen ETL MongoDB/Kafka

`@kozen/etl-mk` is a [Kozen](https://github.com/kozen-labs) module that connects MongoDB change streams to Kafka topics and Kafka topics to MongoDB collections. Each direction is controlled by an independent delegate file. Define one delegate to run a single direction, or define both for a full bidirectional pipeline.

---

## ⚙️ Installation

```bash
npm install @kozen/etl-mk
```

Requires Node.js 18 or later. `kafkajs` and `mongodb` are bundled as runtime dependencies.

---

## 🚀 Quick start

### MongoDB → Kafka (MK pipeline)

```bash
KOZEN_ETL_MK_SOURCE_URI=mongodb+srv://user:pass@cluster.mongodb.net/
KOZEN_ETL_MK_SOURCE_DATABASE=mydb
KOZEN_ETL_MK_SOURCE_COLLECTION=orders
KOZEN_ETL_MK_DESTINATION_BROKERS=broker1:9092,broker2:9092
KOZEN_ETL_MK_DESTINATION_TOPIC=orders.events
KOZEN_ETL_MK_DELEGATE_FILE=/app/delegates/orders.mjs
KOZEN_LOG_LEVEL=INFO
```

```bash
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env
```

Every change on `orders` is transformed by the MK delegate and published to `orders.events`.

### Kafka → MongoDB (KM pipeline)

```bash
KOZEN_ETL_KM_SOURCE_BROKERS=broker1:9092,broker2:9092
KOZEN_ETL_KM_SOURCE_TOPIC=orders.events
KOZEN_ETL_KM_DESTINATION_URI=mongodb+srv://user:pass@cluster.mongodb.net/
KOZEN_ETL_KM_DESTINATION_DATABASE=mydb
KOZEN_ETL_KM_DESTINATION_COLLECTION=orders_archive
KOZEN_ETL_KM_DELEGATE_FILE=/app/delegates/archive.mjs
KOZEN_LOG_LEVEL=INFO
```

Every message from `orders.events` is transformed by the KM delegate and written to `orders_archive`.

Both pipelines can run simultaneously — configure `KOZEN_ETL_MK_*` and `KOZEN_ETL_KM_*` in the same `.env` file. Each direction operates independently and may use different MongoDB instances or Kafka clusters.

Ready-to-use environment file templates are in [`cfg/`](cfg/).

---

## ⚙️ Configuration reference

Variables are prefixed by pipeline direction. `KOZEN_ETL_MK_*` configures MongoDB→Kafka; `KOZEN_ETL_KM_*` configures Kafka→MongoDB. For the full list of CLI flags run `etl:help`.

### MK pipeline (MongoDB → Kafka)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `KOZEN_ETL_MK_SOURCE_URI` | `--mk.source.uri` | (required) | MongoDB connection string |
| `KOZEN_ETL_MK_SOURCE_DATABASE` | `--mk.source.database` | (required) | Database to watch |
| `KOZEN_ETL_MK_SOURCE_COLLECTION` | `--mk.source.collection` | (required) | Collection to watch |
| `KOZEN_ETL_MK_DESTINATION_BROKERS` | `--mk.destination.brokers` | (required) | Comma-separated broker list |
| `KOZEN_ETL_MK_DESTINATION_TOPIC` | `--mk.destination.topic` | (required) | Kafka topic to publish to |
| `KOZEN_ETL_MK_DESTINATION_CLIENT_ID` | `--mk.destination.clientId` | `etl-mk` | Kafka client ID |
| `KOZEN_ETL_MK_DESTINATION_SSL` | `--mk.destination.ssl` | `false` | Enable TLS for Kafka |
| `KOZEN_ETL_MK_DELEGATE_FILE` | `--mk.delegateFile` | (none) | Delegate path — enables this pipeline |
| `KOZEN_ETL_MK_DELEGATE_KEY` | `--mk.delegateKey` | `etl-mk:delegate:source` | IoC key for delegate |
| `KOZEN_ETL_MK_DLQ_TOPIC` | `--mk.dlqTopic` | `<topic>-dlq` | Dead-letter Kafka topic |

### KM pipeline (Kafka → MongoDB)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `KOZEN_ETL_KM_SOURCE_BROKERS` | `--km.source.brokers` | (required) | Comma-separated broker list |
| `KOZEN_ETL_KM_SOURCE_TOPIC` | `--km.source.topic` | (required) | Kafka topic to consume |
| `KOZEN_ETL_KM_SOURCE_GROUP_ID` | `--km.source.groupId` | `etl-mk-group` | Consumer group ID |
| `KOZEN_ETL_KM_SOURCE_CLIENT_ID` | `--km.source.clientId` | `etl-km` | Kafka client ID |
| `KOZEN_ETL_KM_SOURCE_SSL` | `--km.source.ssl` | `false` | Enable TLS for Kafka |
| `KOZEN_ETL_KM_DESTINATION_URI` | `--km.destination.uri` | (required) | MongoDB connection string |
| `KOZEN_ETL_KM_DESTINATION_DATABASE` | `--km.destination.database` | (required) | Database to write to |
| `KOZEN_ETL_KM_DESTINATION_COLLECTION` | `--km.destination.collection` | (required) | Collection to write to |
| `KOZEN_ETL_KM_DELEGATE_FILE` | `--km.delegateFile` | (none) | Delegate path — enables this pipeline |
| `KOZEN_ETL_KM_DELEGATE_KEY` | `--km.delegateKey` | `etl-mk:delegate:destination` | IoC key for delegate |
| `KOZEN_ETL_KM_DESTINATION_WRITE_MODE` | `--km.writeMode` | `insert` | `insert` or `upsert` |
| `KOZEN_ETL_KM_DLQ_TOPIC` | `--km.dlqTopic` | `<topic>-dlq` | Dead-letter Kafka topic |
| `KOZEN_ETL_KM_RETRY_ATTEMPTS` | `--km.retryAttempts` | `3` | Retries before DLQ routing |
| `KOZEN_ETL_KM_RETRY_DELAY_MS` | `--km.retryDelayMs` | `1000` | Initial backoff delay (ms) |

### Common

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `KOZEN_ETL_DELEGATE_TYPE` | `--delegateType` | auto-detect | `esm` or `cjs` |
| `KOZEN_LOG_LEVEL` | — | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `KOZEN_LOG_TYPE` | — | `object` | `object` or `json` |

---

## 🏗️ Delegate pattern

A delegate is a plain JavaScript or TypeScript module. The pipeline loads it via the Kozen IoC container at startup.

### Source delegate (MongoDB → Kafka)

Export functions named after MongoDB change stream operation types. The return value becomes the Kafka message payload. Return `null` or `undefined` to skip the event.

```javascript
// delegates/orders.mjs

export async function insert(change, tools) {
  return {
    id:     change.fullDocument._id.toString(),
    status: change.fullDocument.status
  };
}

export async function update(change, tools) {
  if (!change.updateDescription?.updatedFields?.status) return null;
  return { id: change.documentKey._id.toString(), status: change.updateDescription.updatedFields.status };
}
// Omit delete/replace handlers to skip those operation types.
```

Available operation types: `insert`, `update`, `replace`, `delete`, `drop`, `rename`, `invalidate`. Use `on` or `default` as a catch-all.

Extra tools available in the source delegate:

```javascript
tools.setMessageKey('custom-key');              // override the Kafka message key
tools.setMessageHeaders({ 'x-source': 'etl' }); // add Kafka message headers
```

### Destination delegate (Kafka → MongoDB)

Export a `message`, `on`, or `default` function. The return value is written to the MongoDB collection. Return `null` or `undefined` to skip the write.

```javascript
// delegates/archive.mjs

export async function message(msg, tools) {
  return { ...msg, archivedAt: new Date(), source: tools.collectionName };
}
```

The `tools` object in both delegates provides `db`, `collection`, `dbName`, `collectionName`, `flow`, and `assistant` (the Kozen IoC container).

### Module format

Use `.mjs` for ESM delegates and `.cjs` for CommonJS. Set `KOZEN_ETL_DELEGATE_TYPE` to override auto-detection.

---

## 🚨 Dead-letter handling

When a delegate throws or a write fails after all retries, the failed event is routed to the dead-letter Kafka topic (`KOZEN_ETL_MK_DLQ_TOPIC` / `KOZEN_ETL_KM_DLQ_TOPIC`, default `<topic>-dlq`). The payload shape is `{ originalPayload | originalMessage, error, flow, timestamp }`.

For Kafka→MongoDB failures, the pipeline retries up to `KOZEN_ETL_KM_RETRY_ATTEMPTS` times with exponential backoff (`retryDelayMs × attempt`). The Kafka offset is committed only after a successful write or DLQ routing, ensuring at-least-once delivery.

The `flow` field in every dead-letter record matches the corresponding log entries for end-to-end traceability.

---

## 🔧 Deployment

### PM2

```javascript
// ecosystem.config.js
module.exports = {
  apps: [{
    name: 'orders-etl',
    script: 'node_modules/@kozen/engine/dist/bin/kozen.js',
    args: '--moduleLoad=@kozen/etl-mk --action=etl:start',
    env: {
      KOZEN_ETL_MK_SOURCE_URI:            process.env.MONGO_URI,
      KOZEN_ETL_MK_SOURCE_DATABASE:       'production',
      KOZEN_ETL_MK_SOURCE_COLLECTION:     'orders',
      KOZEN_ETL_MK_DESTINATION_BROKERS:   process.env.KAFKA_BROKERS,
      KOZEN_ETL_MK_DESTINATION_TOPIC:     'orders.events',
      KOZEN_ETL_MK_DELEGATE_FILE:         '/opt/app/delegates/orders.mjs',
      KOZEN_ETL_KM_SOURCE_BROKERS:        process.env.KAFKA_BROKERS,
      KOZEN_ETL_KM_SOURCE_TOPIC:          'orders.events',
      KOZEN_ETL_KM_DESTINATION_URI:       process.env.MONGO_URI,
      KOZEN_ETL_KM_DESTINATION_DATABASE:  'production',
      KOZEN_ETL_KM_DESTINATION_COLLECTION:'orders_archive',
      KOZEN_ETL_KM_DELEGATE_FILE:         '/opt/app/delegates/archive.mjs',
      KOZEN_LOG_LEVEL:              'INFO'
    },
    restart_delay: 5000,
    max_restarts: 10
  }]
};
```

### Docker

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
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

## 🛠️ Development

```bash
npm install
npx tsc --noEmit                              # type-check
npm run build                                 # compile + copy assets to dist/
npm run dev -- --action=etl:help              # run with ts-node
npm run dev -- --action=etl:start --envFile=cfg/env.bidirectional.example
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:validate --envFile=.env
```

Open `.vscode/launch.json` in VS Code to debug with breakpoints using either the TypeScript (`ts-node`) or compiled JavaScript configuration.

The module entry point is [src/index.ts](src/index.ts). The compiled output is written to `dist/`.

---

## 📚 References

- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [kafkajs — Kafka client for Node.js](https://github.com/tulios/kafkajs)
- [@kozen/trigger — Self-hosted MongoDB triggers](https://github.com/mongodb-industry-solutions/kozen-trigger)
- [@kozen/engine — Kozen Task Execution Framework](https://github.com/kozen-labs/engine)
