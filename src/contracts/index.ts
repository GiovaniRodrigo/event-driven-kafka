export * from './envelope';
export * from './events/order';
export * from './events/payment';
export * from './events/inventory';
export * from './events/fraud';
export * from './events/shipping';
export * from './events/notification';
export * from './events/saga';

/**
 * Event Types constant enum.
 */
export const EventTypes = {
  // Order
  OrderCreated: 'OrderCreated',
  OrderCancelled: 'OrderCancelled',

  // Payment
  PaymentRequested: 'PaymentRequested',
  PaymentAuthorized: 'PaymentAuthorized',
  PaymentRejected: 'PaymentRejected',
  PaymentRefundRequested: 'PaymentRefundRequested',
  PaymentRefunded: 'PaymentRefunded',

  // Inventory
  InventoryReservationRequested: 'InventoryReservationRequested',
  InventoryReserved: 'InventoryReserved',
  InventoryReservationFailed: 'InventoryReservationFailed',
  InventoryReleased: 'InventoryReleased',

  // Fraud
  FraudCheckRequested: 'FraudCheckRequested',
  FraudApproved: 'FraudApproved',
  FraudRejected: 'FraudRejected',

  // Shipping
  ShipmentRequested: 'ShipmentRequested',
  ShipmentCreated: 'ShipmentCreated',
  ShipmentFailed: 'ShipmentFailed',

  // Notification
  NotificationRequested: 'NotificationRequested',
  NotificationSent: 'NotificationSent',

  // Saga Lifecycle
  OrderCompleted: 'OrderCompleted',
  OrderFailed: 'OrderFailed',
} as const;

export type EventType = (typeof EventTypes)[keyof typeof EventTypes];
