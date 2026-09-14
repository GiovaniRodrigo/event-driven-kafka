import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { http, HttpResponse } from 'msw';
import { renderApp } from '@/test/render-app';
import { server } from '@/test/msw-server';

vi.mock('@/lib/realtime', async () => {
  const { fakeSocket } = await import('@/test/fake-socket');
  return { getSocket: () => fakeSocket };
});

async function openDialog(user: ReturnType<typeof userEvent.setup>) {
  await screen.findByRole('table'); // wait for the overview to settle
  await user.click(screen.getByRole('button', { name: /new order/i }));
  return screen.findByRole('dialog');
}

describe('Create order flow', () => {
  it('shows validation feedback when the form is empty', async () => {
    const user = userEvent.setup();
    renderApp('/');
    await openDialog(user);

    await user.click(screen.getByRole('button', { name: /submit order/i }));

    expect(await screen.findByText(/Check the form/i)).toBeInTheDocument();
  });

  it('shows a success toast on a 202 accepted order', async () => {
    const user = userEvent.setup();
    renderApp('/');
    const dialog = await openDialog(user);

    await user.type(screen.getByLabelText(/user id/i), 'demo-user');
    await user.type(screen.getByLabelText(/item 1 name/i), 'Widget');
    await user.type(screen.getByLabelText(/item 1 price/i), '9.99');
    await user.click(within(dialog).getByRole('button', { name: /submit order/i }));

    expect(await screen.findByText(/Order accepted/i)).toBeInTheDocument();
  });

  it('surfaces the API 400 as an inline error', async () => {
    server.use(http.post('*/api/orders', () => new HttpResponse(null, { status: 400 })));
    const user = userEvent.setup();
    renderApp('/');
    const dialog = await openDialog(user);

    await user.type(screen.getByLabelText(/user id/i), 'demo-user');
    await user.type(screen.getByLabelText(/item 1 name/i), 'Widget');
    await user.type(screen.getByLabelText(/item 1 price/i), '9.99');
    await user.click(within(dialog).getByRole('button', { name: /submit order/i }));

    expect(await screen.findByText(/The API rejected the order \(400\)/i)).toBeInTheDocument();
  });
});
