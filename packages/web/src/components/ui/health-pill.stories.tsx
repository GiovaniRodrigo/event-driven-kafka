import type { Meta, StoryObj } from '@storybook/react';
import { HealthPill } from './health-pill';

const meta: Meta<typeof HealthPill> = {
  title: 'Components/HealthPill',
  component: HealthPill,
  tags: ['autodocs'],
  args: { health: 'healthy', label: 'Payment' },
  argTypes: { health: { control: 'select', options: ['healthy', 'degraded', 'down'] } },
};
export default meta;

type Story = StoryObj<typeof HealthPill>;

export const Healthy: Story = {};
export const Degraded: Story = { args: { health: 'degraded', label: 'Inventory' } };
export const Down: Story = { args: { health: 'down', label: 'Notification' } };

export const AllStates: Story = {
  render: () => (
    <div className="flex flex-col items-start gap-3">
      <HealthPill label="Payment" health="healthy" />
      <HealthPill label="Inventory" health="degraded" />
      <HealthPill label="Notification" health="down" />
    </div>
  ),
};
