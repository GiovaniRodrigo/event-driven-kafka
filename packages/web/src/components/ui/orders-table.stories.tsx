import type { Meta, StoryObj } from '@storybook/react';
import type { OrderListItem } from '@kafka-demo/contracts';
import { OrdersTable } from './orders-table';

const ORDERS: OrderListItem[] = [
  { order_id: 'a1b2c3d4e5f60718', user_id: 'user-204', status: 'completed', total_amount: 149.9, created_at: new Date().toISOString() },
  { order_id: 'b2c3d4e5f6071829', user_id: 'user-118', status: 'inventory_reserved', total_amount: 89.5, created_at: new Date().toISOString() },
  { order_id: 'c3d4e5f607182930', user_id: 'user-771', status: 'payment_processing', total_amount: 320, created_at: new Date().toISOString() },
  { order_id: 'd4e5f60718293041', user_id: 'user-052', status: 'failed', total_amount: 42.25, created_at: new Date().toISOString() },
];

const meta: Meta<typeof OrdersTable> = {
  title: 'Components/OrdersTable',
  component: OrdersTable,
  tags: ['autodocs'],
  args: { orders: ORDERS },
};
export default meta;

type Story = StoryObj<typeof OrdersTable>;

export const Default: Story = {};
export const Selectable: Story = { args: { onSelect: (id) => alert(`Open ${id}`) } };
export const Empty: Story = { args: { orders: [] } };
