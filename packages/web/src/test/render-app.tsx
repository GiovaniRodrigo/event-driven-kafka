import type { ReactElement } from 'react';
import { render } from '@testing-library/react';
import { RouterProvider, createMemoryRouter } from 'react-router-dom';
import { SWRConfig } from 'swr';
import { routes } from '@/App';
import { ThemeProvider } from '@/components/theme-provider';
import { I18nProvider } from '@/i18n';
import { ToastProvider } from '@/components/ui';

/**
 * Renders the real app at `initialPath` through a memory router. Uses SWR's
 * default cache (not a scoped provider) so the app's global `mutate`-based
 * realtime patches take effect under test exactly as in production; the cache
 * is cleared between tests in setup.ts. This is the single seam the frontend
 * tests exercise (REST via MSW, socket via fakeSocket).
 */
export function renderApp(initialPath = '/') {
  const router = createMemoryRouter(routes, { initialEntries: [initialPath] });
  return render(
    <SWRConfig value={{ dedupingInterval: 0 }}>
      <I18nProvider initialLang="en">
        <ThemeProvider>
          <ToastProvider>
            <RouterProvider router={router} />
          </ToastProvider>
        </ThemeProvider>
      </I18nProvider>
    </SWRConfig>,
  );
}

export function wrap(ui: ReactElement) {
  return (
    <SWRConfig value={{ dedupingInterval: 0 }}>
      <I18nProvider initialLang="en">
        <ThemeProvider>
          <ToastProvider>{ui}</ToastProvider>
        </ThemeProvider>
      </I18nProvider>
    </SWRConfig>
  );
}
