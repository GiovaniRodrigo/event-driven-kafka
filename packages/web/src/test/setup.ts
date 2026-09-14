import '@testing-library/jest-dom/vitest';
import { afterAll, afterEach, beforeAll } from 'vitest';
import { cleanup } from '@testing-library/react';
import { mutate } from 'swr';
import { server } from './msw-server';
import { fakeSocket } from './fake-socket';

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));

afterEach(async () => {
  cleanup();
  server.resetHandlers();
  fakeSocket.reset();
  // Clear SWR's default cache so tests don't leak fetched data into each other.
  await mutate(() => true, undefined, { revalidate: false });
});

afterAll(() => server.close());
