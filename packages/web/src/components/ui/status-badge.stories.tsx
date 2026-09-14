import type { Meta, StoryObj } from '@storybook/react';
import type { OrderStatus } from '@kafka-demo/contracts';
import { StatusBadge } from './status-badge';

const ALL: OrderStatus[] = [
  'pending',
  'payment_processing',
  'payment_approved',
  'inventory_reserved',
  'completed',
  'failed',
];

const meta: Meta<typeof StatusBadge> = {
  title: 'Components/StatusBadge',
  component: StatusBadge,
  tags: ['autodocs'],
  args: { status: 'completed' },
  argTypes: { status: { control: 'select', options: ALL } },
};
export default meta;

type Story = StoryObj<typeof StatusBadge>;

export const Default: Story = {};

export const AllStatuses: Story = {
  render: () => (
    <div className="flex flex-wrap gap-2">
      {ALL.map((s) => (
        <StatusBadge key={s} status={s} />
      ))}
    </div>
  ),
};
