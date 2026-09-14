import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderApp } from '@/test/render-app';

// The shell subscribes to the socket; swap it for the fake.
vi.mock('@/lib/realtime', async () => {
  const { fakeSocket } = await import('@/test/fake-socket');
  return { getSocket: () => fakeSocket };
});

describe('i18n', () => {
  it('switches UI copy between EN and PT via the header toggle', async () => {
    const user = userEvent.setup();
    renderApp('/');

    // renderApp seeds English; the primary nav reads "Overview".
    expect(await screen.findByRole('link', { name: /overview/i })).toBeInTheDocument();

    // Toggle to Portuguese; the same nav item re-renders as "Visão geral".
    await user.click(screen.getByRole('button', { name: /language: english/i }));
    expect(await screen.findByRole('link', { name: /visão geral/i })).toBeInTheDocument();
    expect(screen.queryByRole('link', { name: /^overview$/i })).not.toBeInTheDocument();
  });
});
