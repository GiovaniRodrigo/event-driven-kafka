import type { Meta, StoryObj } from '@storybook/react';
import { MetricCard } from './metric-card';

const meta: Meta<typeof MetricCard> = {
  title: 'Components/MetricCard',
  component: MetricCard,
  tags: ['autodocs'],
  args: {
    label: 'Orders / min',
    value: '24',
    delta: 12.4,
    trend: [3, 5, 4, 8, 7, 10, 9, 12],
    tone: 'primary',
  },
  argTypes: {
    tone: { control: 'select', options: ['primary', 'success', 'warning', 'danger', 'info'] },
  },
};
export default meta;

type Story = StoryObj<typeof MetricCard>;

export const Positive: Story = {};
export const Negative: Story = {
  args: { label: 'Consumer lag', value: '18', delta: -6.2, trend: [30, 26, 24, 22, 20, 18], tone: 'warning' },
};
export const NoTrend: Story = { args: { label: 'Completed', value: '312', delta: undefined, trend: undefined, tone: 'success' } };

export const Row: Story = {
  render: () => (
    <div className="grid max-w-3xl gap-4 sm:grid-cols-3">
      <MetricCard label="Orders / min" value="24" delta={12.4} trend={[3, 5, 4, 8, 10, 12]} tone="primary" />
      <MetricCard label="Events processed" value="1,208" delta={4.1} trend={[40, 48, 55, 60, 64]} tone="info" />
      <MetricCard label="Consumer lag" value="18" delta={-6.2} trend={[30, 26, 22, 20, 18]} tone="warning" />
    </div>
  ),
};
