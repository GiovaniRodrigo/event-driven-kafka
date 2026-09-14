import type { Meta, StoryObj } from '@storybook/react';
import { Alert } from './alert';

const meta: Meta<typeof Alert> = {
  title: 'Components/Alert',
  component: Alert,
  tags: ['autodocs'],
  args: { tone: 'info', title: 'Heads up', children: 'Something worth noting.' },
  argTypes: { tone: { control: 'select', options: ['info', 'success', 'warning', 'danger'] } },
};
export default meta;

type Story = StoryObj<typeof Alert>;

export const Info: Story = {};
export const Success: Story = { args: { tone: 'success', title: 'Order accepted', children: '202 Accepted.' } };
export const Warning: Story = { args: { tone: 'warning', title: 'Degraded', children: 'A consumer is falling behind.' } };
export const Danger: Story = { args: { tone: 'danger', title: 'API unreachable', children: 'Check that the backend is running.' } };
