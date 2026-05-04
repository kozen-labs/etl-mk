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

### MongoDB → Kafka

```bash
ETL_MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
ETL_MONGO_DATABASE=mydb
ETL_MONGO_COLLECTION=orders
ETL_KAFKA_BROKERS=broker1:9092,broker2:9092
ETL_KAFKA_TOPIC=orders.events
ETL_SOURCE_DELEGATE_FILE=/app/delegates/orders.mjs
KOZEN_LOG_LEVEL=INFO
```

```bash
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env
```

Every change on `orders` is transformed by the source delegate and published to `orders.events`.

### Kafka → MongoDB

```bash
ETL_MONGO_URI=mongodb+srv://user:pass@cluster.mongodb.net/
ETL_MONGO_DATABASE=mydb
ETL_MONGO_COLLECTION=orders_archive
ETL_KAFKA_BROKERS=broker1:9092,broker2:9092
ETL_KAFKA_TOPIC=orders.events
ETL_KAFKA_GROUP_ID=etl-mk-group
ETL_DESTINATION_DELEGATE_FILE=/app/delegates/archive.mjs
KOZEN_LOG_LEVEL=INFO
```

Every message from `orders.events` is transformed by the destination delegate and written to `orders_archive`.

Ready-to-use environment file templates are in [`cfg/`](cfg/).

---

## ⚙️ Configuration reference

All variables have an equivalent CLI flag: `ETL_MONGO_URI` maps to `--mongo.uri`, `ETL_KAFKA_TOPIC` maps to `--kafka.topic`, and so on. For the full list run `etl:help`.

### Connection

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_MONGO_URI` | `--mongo.uri` | (required) | MongoDB connection string |
| `ETL_MONGO_DATABASE` | `--mongo.database` | (required) | Database name |
| `ETL_MONGO_COLLECTION` | `--mongo.collection` | (required) | Collection name |
| `ETL_KAFKA_BROKERS` | `--kafka.brokers` | (required) | Comma-separated broker list |
| `ETL_KAFKA_TOPIC` | `--kafka.topic` | (required) | Kafka topic |
| `ETL_KAFKA_GROUP_ID` | `--kafka.groupId` | `etl-mk-group` | Consumer group ID |
| `ETL_KAFKA_CLIENT_ID` | `--kafka.clientId` | `etl-mk` | Kafka client ID |
| `ETL_KAFKA_SSL` | `--kafka.ssl` | `false` | Enable TLS |

### Delegates

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_SOURCE_DELEGATE_FILE` | `--sourceDelegateFile` | (none) | Delegate path — enables MongoDB→Kafka |
| `ETL_DESTINATION_DELEGATE_FILE` | `--destinationDelegateFile` | (none) | Delegate path — enables Kafka→MongoDB |
| `ETL_DELEGATE_TYPE` | `--delegateType` | auto-detect | `esm` or `cjs` |

### Pipeline

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_WRITE_MODE` | `--writeMode` | `insert` | `insert` or `upsert` |
| `ETL_DLQ_TOPIC` | `--dlqTopic` | `<topic>-dlq` | Dead-letter Kafka topic |
| `ETL_RETRY_ATTEMPTS` | `--retryAttempts` | `3` | Retries before DLQ routing |
| `ETL_RETRY_DELAY_MS` | `--retryDelayMs` | `1000` | Initial backoff delay (ms) |
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

Use `.mjs` for ESM delegates and `.cjs` for CommonJS. Set `ETL_DELEGATE_TYPE` to override auto-detection.

---

## 🚨 Dead-letter handling

When a delegate throws or a write fails after all retries, the failed event is routed to the dead-letter Kafka topic (`ETL_DLQ_TOPIC`, default `<topic>-dlq`). The payload shape is `{ originalPayload | originalMessage, error, flow, timestamp }`.

For Kafka→MongoDB failures, the pipeline retries up to `ETL_RETRY_ATTEMPTS` times with exponential backoff (`retryDelayMs × attempt`). The Kafka offset is committed only after a successful write or DLQ routing, ensuring at-least-once delivery.

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
      ETL_MONGO_URI:                process.env.MONGO_URI,
      ETL_MONGO_DATABASE:           'production',
      ETL_MONGO_COLLECTION:         'orders',
      ETL_KAFKA_BROKERS:            process.env.KAFKA_BROKERS,
      ETL_KAFKA_TOPIC:              'orders.events',
      ETL_SOURCE_DELEGATE_FILE:     '/opt/app/delegates/orders.mjs',
      ETL_DESTINATION_DELEGATE_FILE:'/opt/app/delegates/archive.mjs',
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
  -e ETL_MONGO_URI="mongodb+srv://..." \
  -e ETL_MONGO_DATABASE=production \
  -e ETL_MONGO_COLLECTION=orders \
  -e ETL_KAFKA_BROKERS="broker1:9092" \
  -e ETL_KAFKA_TOPIC=orders.events \
  -e ETL_SOURCE_DELEGATE_FILE=/app/delegates/orders.mjs \
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
