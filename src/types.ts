export type OrderStatus =
  | 'pending'
  | 'payment_processing'
  | 'payment_approved'
  | 'inventory_reserved'
  | 'completed'
  | 'failed';

export interface Order {
  id: string;
  user_id: string;
  items: OrderItem[];
  status: OrderStatus;
  total_amount: number;
  created_at: Date;
  updated_at: Date;
}

export interface OrderItem {
  sku: string;
  name: string;
  price: number;
  quantity: number;
}

export interface OrderEvent {
  event_type: string;
  topic: string;
  timestamp: string;
}

export interface KafkaEvent {
  event_id: string;
  event_type: string;
  correlation_id: string;
  timestamp: string;
  source_service: string;
  payload: any;
}

export interface PaymentEvent {
  event_id: string;
  order_id: string;
  user_id: string;
  amount: number;
  timestamp: string;
  correlation_id: string;
}

export interface InventoryEvent {
  event_id: string;
  order_id: string;
  items: OrderItem[];
  timestamp: string;
  correlation_id: string;
}
