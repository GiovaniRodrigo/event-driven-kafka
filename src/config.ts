import { PoolConfig } from 'pg';

export const kafkaConfig = {
  brokers: (process.env.KAFKA_BROKER || 'localhost:9092').split(','),
  clientId: 'event-driven-api',
  connectionTimeout: 10000,
  requestTimeout: 30000,
};

export const producerConfig = {
  idempotent: true,
  maxInFlightRequests: 5,
  compression: 1,
  batchSize: 16384,
  lingerMs: 10,
  acks: -1,
  timeout: 30000,
};

export const consumerConfig = {
  sessionTimeout: 30000,
  heartbeatInterval: 10000,
  rebalanceTimeout: 60000,
  allowAutoTopicCreation: false,
};

export const topics = {
  orders: {
    name: 'orders',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=snappy', 'retention.ms=604800000'],
  },
  payments: {
    name: 'payments',
    partitions: 3,
    replicationFactor: 1,
  },
  inventory: {
    name: 'inventory',
    partitions: 3,
    replicationFactor: 1,
  },
  notifications: {
    name: 'notifications',
    partitions: 1,
    replicationFactor: 1,
  },
  dlq: {
    name: 'dlq',
    partitions: 1,
    replicationFactor: 1,
    config: ['retention.ms=2592000000'],
  },
};

const poolTuning = {
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
};

// Prefer a single DATABASE_URL (as supplied by docker-compose) when present,
// otherwise fall back to the discrete DB_* variables.
export const databaseConfig: PoolConfig = process.env.DATABASE_URL
  ? { connectionString: process.env.DATABASE_URL, ...poolTuning }
  : {
      host: process.env.DB_HOST || 'localhost',
      port: parseInt(process.env.DB_PORT || '5432', 10),
      database: process.env.DB_NAME || 'event_driven',
      user: process.env.DB_USER || 'postgres',
      password: process.env.DB_PASSWORD || 'postgres',
      ...poolTuning,
    };
