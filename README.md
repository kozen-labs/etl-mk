# 🔄 Kozen ETL MongoDB/Kafka — Bi-directional MongoDB ↔ Kafka ETL

`@kozen/etl-mk` is a [Kozen](https://github.com/kozen-labs) module that connects MongoDB change streams to Kafka topics and Kafka topics to MongoDB collections. It eliminates the boilerplate of writing change-stream listeners, Kafka producers, and Kafka consumers by hand, replacing all of it with a single `.env` file and an optional delegate function.

---

## ⚙️ Installation

```bash
npm install @kozen/etl-mk
```

Requires Node.js 18 or later. `kafkajs` and `mongodb` are bundled as runtime dependencies.

---

## 🚀 Quick start

### mongo-to-kafka (passthrough, no delegate)

Create a `.env` file:

```bash
KOZEN_MODULE_LOAD=@kozen/etl-mk
ETL_MODE=mongo-to-kafka

# Source: MongoDB collection to watch
ETL_SOURCE_URI=mongodb+srv://user:pass@cluster.mongodb.net/
ETL_SOURCE_DATABASE=mydb
ETL_SOURCE_COLLECTION=orders

# Destination: Kafka topic to publish to
ETL_DESTINATION_BROKERS=broker1:9092,broker2:9092
ETL_DESTINATION_TOPIC=orders.created

KOZEN_LOG_LEVEL=INFO
```

Start the pipeline:

```bash
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:start --envFile=.env
```

Every insert, update, replace, or delete on `orders` is immediately published to `orders.created`. No code required.

### kafka-to-mongo (passthrough, no delegate)

```bash
KOZEN_MODULE_LOAD=@kozen/etl-mk
ETL_MODE=kafka-to-mongo

# Source: Kafka topic to consume
ETL_SOURCE_BROKERS=broker1:9092,broker2:9092
ETL_SOURCE_TOPIC=orders.created
ETL_SOURCE_GROUP_ID=my-consumer-group

# Destination: MongoDB collection to write to
ETL_DESTINATION_URI=mongodb+srv://user:pass@cluster.mongodb.net/
ETL_DESTINATION_DATABASE=mydb
ETL_DESTINATION_COLLECTION=orders_archive

KOZEN_LOG_LEVEL=INFO
```

Every message consumed from `orders.created` is inserted into `orders_archive`. Again, no code required.

---

## ⚙️ Configuration reference

All variables accept an equivalent CLI flag form: `ETL_SOURCE_URI` maps to `--source.uri`, `ETL_DESTINATION_TOPIC` maps to `--destination.topic`, and so on. Optional variables carry the defaults shown below.

### Common

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_MODE` | `--mode` | (required) | `mongo-to-kafka` or `kafka-to-mongo` |
| `ETL_DELEGATE_FILE` | `--file` | (passthrough) | Absolute path to delegate file |
| `ETL_DELEGATE_TYPE` | `--delegateType` | auto-detect | `esm` or `cjs` |
| `ETL_RETRY_ATTEMPTS` | `--retryAttempts` | `3` | Retry count on transient errors |
| `ETL_RETRY_DELAY_MS` | `--retryDelayMs` | `1000` | Initial backoff delay in milliseconds |
| `KOZEN_LOG_LEVEL` | (none) | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `KOZEN_LOG_TYPE` | (none) | `object` | `object` for human-readable, `json` for log shippers |

### Source: MongoDB (`mongo-to-kafka` mode)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_SOURCE_URI` | `--source.uri` | (required) | MongoDB connection string |
| `ETL_SOURCE_DATABASE` | `--source.database` | (required) | Database to watch |
| `ETL_SOURCE_COLLECTION` | `--source.collection` | (required) | Collection to watch |

### Destination: Kafka (`mongo-to-kafka` mode)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_DESTINATION_BROKERS` | `--destination.brokers` | (required) | Comma-separated broker list |
| `ETL_DESTINATION_TOPIC` | `--destination.topic` | (required) | Target Kafka topic |
| `ETL_DESTINATION_CLIENT_ID` | `--destination.clientId` | `etl-mk` | Kafka producer client ID |
| `ETL_DESTINATION_DLQ_TOPIC` | `--destination.dlqTopic` | `<topic>-dlq` | Dead-letter topic |
| `ETL_DESTINATION_SSL` | `--destination.ssl` | `false` | Enable TLS for Kafka connection |

### Source: Kafka (`kafka-to-mongo` mode)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_SOURCE_BROKERS` | `--source.brokers` | (required) | Comma-separated broker list |
| `ETL_SOURCE_TOPIC` | `--source.topic` | (required) | Topic to consume |
| `ETL_SOURCE_GROUP_ID` | `--source.groupId` | `etl-mk-group` | Consumer group ID |
| `ETL_SOURCE_CLIENT_ID` | `--source.clientId` | `etl-mk` | Kafka consumer client ID |
| `ETL_SOURCE_SSL` | `--source.ssl` | `false` | Enable TLS for Kafka connection |

### Destination: MongoDB (`kafka-to-mongo` mode)

| Variable | CLI flag | Default | Description |
|---|---|---|---|
| `ETL_DESTINATION_URI` | `--destination.uri` | (required) | MongoDB connection string |
| `ETL_DESTINATION_DATABASE` | `--destination.database` | (required) | Target database |
| `ETL_DESTINATION_COLLECTION` | `--destination.collection` | (required) | Target collection |
| `ETL_DESTINATION_WRITE_MODE` | `--destination.writeMode` | `insert` | `insert` or `upsert` |
| `ETL_DESTINATION_DLQ_COLLECTION` | `--destination.dlqCollection` | `<collection>_dlq` | Dead-letter collection |

---

## 🏗️ Delegate pattern

A delegate is a plain JavaScript or TypeScript module where each exported function name maps to an event type. The module loads it at startup from `ETL_DELEGATE_FILE`. If no file is specified, the built-in passthrough forwards the raw payload unchanged.

### mongo-to-kafka delegate

Each handler receives the raw MongoDB change stream event and a `tools` object. The return value becomes the Kafka message payload. Returning `null` or `undefined` skips the event without publishing.

```javascript
// delegates/orders.mjs

export async function insert(change, tools) {
  // Transform: publish only the fields downstream consumers need
  return {
    id:        change.fullDocument._id.toString(),
    total:     change.fullDocument.total,
    status:    change.fullDocument.status,
    createdAt: change.fullDocument.createdAt
  };
}

export async function update(change, tools) {
  // Publish only status-change updates; skip price-only updates
  if (!change.updateDescription?.updatedFields?.status) return null;
  return {
    id:     change.documentKey._id.toString(),
    status: change.updateDescription.updatedFields.status
  };
}

// delete and replace handlers are optional; omit to skip those event types
```

The `tools` object extends the `@kozen/trigger` `ITriggerTools` interface with two extra methods:

```typescript
tools.setMessageKey('custom-key');             // override the Kafka message key
tools.setMessageHeaders({ 'x-source': 'etl' }); // add custom Kafka message headers
```

The default message key is `change.documentKey._id.toString()`.

### kafka-to-mongo delegate

Each handler receives the deserialized message value and a `tools` object. The return value is inserted into the destination collection. Returning `null` or `undefined` skips the write.

```javascript
// delegates/archive.mjs

export async function message(msg, tools) {
  // Enrich the document before storing it
  return {
    ...msg,
    archivedAt: new Date(),
    source: tools.collectionName
  };
}
```

Accepted handler names (in resolution order): `message`, `on`, `default`. The `tools` object provides `db`, `collection`, `dbName`, `collectionName`, `flow`, and `assistant` (the Kozen IoC container).

### ESM vs CommonJS delegates

The module detects the delegate format from the file extension. Use `.mjs` for ECMAScript Module (ESM) delegates and `.cjs` for CommonJS (CJS) delegates. Set `ETL_DELEGATE_TYPE=esm` or `ETL_DELEGATE_TYPE=cjs` to override detection.

---

## 🚨 Dead-letter handling

When a delegate throws or a delivery fails after all retries, the module routes the event to a dead-letter sink instead of dropping it.

| Mode | Dead-letter sink | Default name | Payload shape |
|---|---|---|---|
| `mongo-to-kafka` | Kafka topic | `<topic>-dlq` | `{ originalPayload, error, flow, timestamp }` |
| `kafka-to-mongo` | MongoDB collection | `<collection>_dlq` | `{ originalMessage, error, flow, timestamp }` |

Override the default sink name with `ETL_DESTINATION_DLQ_TOPIC` or `ETL_DESTINATION_DLQ_COLLECTION`.

Every dead-letter record carries the `flow` correlation identifier (ID), which matches the corresponding log entries. Use it to trace a specific failed event end-to-end across logs and the dead-letter sink.

---

## 🔧 Deployment

### PM2

```javascript
// ecosystem.config.js
module.exports = {
  apps: [{
    name: 'orders-to-kafka',
    script: 'node_modules/@kozen/engine/dist/bin/kozen.js',
    args: '--action=etl:start',
    env: {
      KOZEN_MODULE_LOAD: '@kozen/etl-mk',
      ETL_MODE: 'mongo-to-kafka',
      ETL_SOURCE_URI: process.env.MONGO_URI,
      ETL_SOURCE_DATABASE: 'production',
      ETL_SOURCE_COLLECTION: 'orders',
      ETL_DESTINATION_BROKERS: process.env.KAFKA_BROKERS,
      ETL_DESTINATION_TOPIC: 'orders.created',
      ETL_DELEGATE_FILE: '/opt/app/delegates/orders.mjs',
      KOZEN_LOG_LEVEL: 'INFO'
    },
    restart_delay: 5000,
    max_restarts: 10
  }]
};
```

```bash
pm2 start ecosystem.config.js
pm2 save && pm2 startup
```

### Docker

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY delegates/ ./delegates/
CMD ["npx", "kozen", "--action=etl:start"]
```

```bash
docker run -d \
  -e KOZEN_MODULE_LOAD=@kozen/etl-mk \
  -e ETL_MODE=mongo-to-kafka \
  -e ETL_SOURCE_URI="mongodb+srv://..." \
  -e ETL_SOURCE_DATABASE=production \
  -e ETL_SOURCE_COLLECTION=orders \
  -e ETL_DESTINATION_BROKERS="broker1:9092" \
  -e ETL_DESTINATION_TOPIC=orders.created \
  -e ETL_DELEGATE_FILE=/app/delegates/orders.mjs \
  -v /host/delegates:/app/delegates \
  my-etl-mk-image
```

### Validate configuration before starting

Run `etl:validate` to check that all required variables are present and report missing ones without starting the pipeline:

```bash
npx kozen --moduleLoad=@kozen/etl-mk --action=etl:validate --envFile=.env
```

---

## 🛠️ Development

```bash
# Install dependencies
npm install

# Type-check without emitting
npx tsc --noEmit

# Build (compile TypeScript + copy config assets to dist/)
npm run build

# Run via ts-node without building (useful during development)
npm run dev -- --action=etl:help
npm run dev -- --action=etl:start --envFile=.env.local
```

The module entry point is [src/index.ts](src/index.ts). The compiled output is written to `dist/` and is the only artifact published to npm.

---

## 📚 References

- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [kafkajs — Kafka client for Node.js](https://github.com/tulios/kafkajs)
- [@kozen/trigger — Self-hosted MongoDB triggers](https://github.com/mongodb-industry-solutions/kozen-trigger)
- [@kozen/engine — Kozen Task Execution Framework](https://github.com/kozen-labs/engine)
