import { describe, it, expect, vi } from 'vitest';
import { act, screen, waitFor, within } from '@testing-library/react';
import { http, HttpResponse } from 'msw';
import { renderApp } from '@/test/render-app';
import { server } from '@/test/msw-server';
import { fakeSocket } from '@/test/fake-socket';

// The view talks to the socket through this module; swap it for the fake.
vi.mock('@/lib/realtime', async () => {
  const { fakeSocket } = await import('@/test/fake-socket');
  return { getSocket: () => fakeSocket };
});

describe('Overview', () => {
  it('renders recent orders with their status badges', async () => {
    renderApp('/');
    const table = await screen.findByRole('table');
    expect(within(table).getByText(/ord_comp/)).toBeInTheDocument();
    expect(within(table).getByText('Completed')).toBeInTheDocument();
    expect(within(table).getByText('Failed')).toBeInTheDocument();
  });

  it('shows an error state when the orders API is unreachable', async () => {
    server.use(http.get('*/api/orders', () => HttpResponse.error()));
    renderApp('/');
    expect(await screen.findByText(/Orders unavailable/i)).toBeInTheDocument();
  });

  it('updates a row live when an order:updated event arrives', async () => {
    renderApp('/');
    const table = await screen.findByRole('table');
    expect(within(table).getAllByText('Failed')).toHaveLength(1);

    // The read-side bridge broadcasts that the completed order now failed.
    act(() => {
      fakeSocket.emitServer('order:updated', {
        order_id: 'ord_completed01',
        status: 'failed',
      });
    });

    await waitFor(() => {
      expect(within(table).getAllByText('Failed')).toHaveLength(2);
    });
  });
});
