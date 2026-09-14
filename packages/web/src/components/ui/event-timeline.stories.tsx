import type { Meta, StoryObj } from '@storybook/react';
import type { OrderEventPayload } from '@kafka-demo/contracts';
import { EventTimeline } from './event-timeline';

const EVENTS: OrderEventPayload[] = [
  { event_type: 'order.created', topic: 'orders', timestamp: new Date(Date.now() - 40000).toISOString() },
  { event_type: 'payment.approved', topic: 'payments', timestamp: new Date(Date.now() - 25000).toISOString() },
  { event_type: 'inventory.reserved', topic: 'inventory', timestamp: new Date(Date.now() - 12000).toISOString() },
  { event_type: 'notification.sent', topic: 'notifications', timestamp: new Date(Date.now() - 4000).toISOString() },
];

const meta: Meta<typeof EventTimeline> = {
  title: 'Components/EventTimeline',
  component: EventTimeline,
  tags: ['autodocs'],
  args: { events: EVENTS },
};
export default meta;

type Story = StoryObj<typeof EventTimeline>;

export const HappyPath: Story = {};
export const Failed: Story = {
  args: {
    failed: true,
    events: [
      { event_type: 'order.created', topic: 'orders', timestamp: new Date(Date.now() - 20000).toISOString() },
      { event_type: 'payment.failed', topic: 'payments', timestamp: new Date(Date.now() - 6000).toISOString() },
    ],
  },
};
export const Empty: Story = { args: { events: [] } };
