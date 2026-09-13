import { Kafka, Producer } from 'kafkajs';
import { kafkaConfig, producerConfig } from '../config';
import { logger } from '../utils/logger';

export abstract class BaseProducer {
  protected producer: Producer;
  protected kafka: Kafka;

  constructor(protected topicName: string) {
    this.kafka = new Kafka(kafkaConfig);
    this.producer = this.kafka.producer(producerConfig);
  }

  async connect() {
    await this.producer.connect();
    logger.info({ event: 'producer_connected', topic: this.topicName });
  }

  async disconnect() {
    await this.producer.disconnect();
    logger.info({ event: 'producer_disconnected', topic: this.topicName });
  }

  async emit(eventType: string, payload: any, key?: string) {
    const startTime = Date.now();

    try {
      const result = await this.producer.send({
        topic: this.topicName,
        messages: [
          {
            key: key || payload.event_id,
            value: JSON.stringify({
              type: eventType,
              ...payload,
            }),
            headers: {
              'correlation-id': payload.correlation_id || '',
              'event-id': payload.event_id || '',
              'source-service': 'order-service',
              timestamp: new Date().toISOString(),
            },
          },
        ],
      });

      const duration = Date.now() - startTime;

      logger.info({
        event: 'message_produced',
        topic: this.topicName,
        type: eventType,
        event_id: payload.event_id,
        partition: result[0].partition,
        offset: result[0].offset,
        duration_ms: duration,
      });

      return result;
    } catch (error) {
      logger.error({
        event: 'produce_error',
        topic: this.topicName,
        type: eventType,
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      throw error;
    }
  }
}
