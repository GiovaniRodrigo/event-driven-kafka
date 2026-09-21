import { PoolConfig } from 'pg';
import { z } from 'zod';

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.coerce.number().default(3000),
  KAFKA_BROKER: z.string().default('localhost:9092'),
  DATABASE_URL: z.string().optional(),
  DB_HOST: z.string().default('localhost'),
  DB_PORT: z.coerce.number().default(5432),
  DB_NAME: z.string().default('event_driven_test'),
  DB_USER: z.string().default('postgres'),
  DB_PASSWORD: z.string().default('postgres'),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
});

export const env = EnvSchema.parse(process.env);

export const kafkaConfig = {
  brokers: env.KAFKA_BROKER.split(','),
  clientId: 'event-driven-api',
  connectionTimeout: 10000,
  requestTimeout: 30000,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
};

export const producerConfig = {
  idempotent: true,
  maxInFlightRequests: 5,
  compression: 1, // GZIP/Snappy
  batchSize: 16384,
  lingerMs: 10,
  acks: -1, // all ISR replicas
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
    name: 'orders.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=producer', 'retention.ms=604800000'],
  },
  payments: {
    name: 'payments.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=producer', 'retention.ms=604800000'],
  },
  inventory: {
    name: 'inventory.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=producer', 'retention.ms=604800000'],
  },
  fraud: {
    name: 'fraud.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=producer', 'retention.ms=604800000'],
  },
  shipping: {
    name: 'shipping.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['compression.type=producer', 'retention.ms=604800000'],
  },
  notifications: {
    name: 'notifications.events',
    partitions: 3,
    replicationFactor: 1,
    config: ['retention.ms=604800000'],
  },
  // Retry topics
  ordersRetry: {
    name: 'orders.retry',
    partitions: 3,
    replicationFactor: 1,
  },
  paymentsRetry: {
    name: 'payments.retry',
    partitions: 3,
    replicationFactor: 1,
  },
  inventoryRetry: {
    name: 'inventory.retry',
    partitions: 3,
    replicationFactor: 1,
  },
  fraudRetry: {
    name: 'fraud.retry',
    partitions: 3,
    replicationFactor: 1,
  },
  shippingRetry: {
    name: 'shipping.retry',
    partitions: 3,
    replicationFactor: 1,
  },
  // Dead Letter Queue
  dlq: {
    name: 'platform.dlq',
    partitions: 1,
    replicationFactor: 1,
    config: ['retention.ms=2592000000'],
  },
  // Platform & Replay
  platform: {
    name: 'platform.events',
    partitions: 1,
    replicationFactor: 1,
  },
  replay: {
    name: 'replay.events',
    partitions: 1,
    replicationFactor: 1,
  },
};

const poolTuning = {
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
};

export const databaseConfig: PoolConfig = env.DATABASE_URL
  ? { connectionString: env.DATABASE_URL, ...poolTuning }
  : {
      host: env.DB_HOST,
      port: env.DB_PORT,
      database: env.DB_NAME,
      user: env.DB_USER,
      password: env.DB_PASSWORD,
      ...poolTuning,
    };
