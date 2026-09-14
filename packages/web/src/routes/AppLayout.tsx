import * as React from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import {
  Activity,
  LayoutDashboard,
  ListOrdered,
  Map as MapIcon,
  Menu,
  Moon,
  Search,
  Sun,
  X,
} from 'lucide-react';
import { useTheme } from '@/components/theme-provider';
import { useI18n } from '@/i18n';
import { useRealtimeSync } from '@/hooks/use-realtime-sync';
import { CreateOrderDialog } from '@/components/create-order-dialog';
import { Button } from '@/components/ui';
import { cn } from '@/lib/utils';

interface NavItem {
  to: string;
  labelKey: string;
  icon: typeof LayoutDashboard;
  end?: boolean;
  enabled: boolean;
}

const NAV: NavItem[] = [
  { to: '/', labelKey: 'nav.overview', icon: LayoutDashboard, end: true, enabled: true },
  { to: '/orders', labelKey: 'nav.orders', icon: ListOrdered, enabled: false },
  { to: '/consumers', labelKey: 'nav.consumers', icon: Activity, enabled: true },
  { to: '/map', labelKey: 'nav.map', icon: MapIcon, enabled: false },
];

/** App shell: left sidebar workspace nav + operations top bar (v0-faithful). */
export function AppLayout() {
  const { theme, toggle } = useTheme();
  const { lang, toggle: toggleLang, t } = useI18n();
  const { connected } = useRealtimeSync();
  const [navOpen, setNavOpen] = React.useState(false);

  const nav = (
    <nav className="flex flex-col gap-1" aria-label={t('shell.nav')}>
      {NAV.map(({ to, labelKey, icon: Icon, end, enabled }) =>
        enabled ? (
          <NavLink
            key={to}
            to={to}
            end={end}
            onClick={() => setNavOpen(false)}
            className={({ isActive }) =>
              cn(
                'inline-flex items-center gap-3 rounded-md px-3 py-2 text-body font-medium transition-colors',
                isActive
                  ? 'bg-surface-muted text-surface-foreground'
                  : 'text-muted-foreground hover:bg-surface-muted hover:text-surface-foreground',
              )
            }
          >
            <Icon className="size-4 shrink-0" /> {t(labelKey)}
          </NavLink>
        ) : (
          <span
            key={to}
            aria-disabled
            className="inline-flex cursor-not-allowed items-center gap-3 rounded-md px-3 py-2 text-body font-medium text-muted-foreground/60"
          >
            <Icon className="size-4 shrink-0" /> {t(labelKey)}
            <span className="ml-auto rounded-full border border-border px-1.5 py-0.5 text-[0.625rem] uppercase tracking-wide text-muted-foreground/70">
              {t('shell.soon')}
            </span>
          </span>
        ),
      )}
    </nav>
  );

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      {/* Sidebar (persistent on lg+, drawer on mobile) */}
      <aside
        className={cn(
          'fixed inset-y-0 left-0 z-50 flex w-64 flex-col border-r border-border bg-surface transition-transform lg:static lg:translate-x-0',
          navOpen ? 'translate-x-0' : '-translate-x-full',
        )}
      >
        <div className="flex items-center justify-between px-5 py-4">
          <div>
            <p className="text-caption uppercase tracking-wide text-muted-foreground">
              {t('shell.workspace')}
            </p>
            <p className="text-heading font-semibold text-surface-foreground">
              {t('shell.platform')}
            </p>
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="lg:hidden"
            onClick={() => setNavOpen(false)}
            aria-label={t('shell.menu')}
          >
            <X />
          </Button>
        </div>
        <div className="flex-1 overflow-y-auto px-3 py-2">{nav}</div>
      </aside>

      {/* Mobile overlay */}
      {navOpen && (
        <button
          type="button"
          aria-label={t('shell.menu')}
          className="fixed inset-0 z-40 bg-background/60 backdrop-blur-sm lg:hidden"
          onClick={() => setNavOpen(false)}
        />
      )}

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="sticky top-0 z-30 border-b border-border bg-background/80 backdrop-blur">
          <div className="flex flex-wrap items-center gap-3 px-4 py-3 sm:px-6">
            <Button
              variant="ghost"
              size="icon"
              className="lg:hidden"
              onClick={() => setNavOpen(true)}
              aria-label={t('shell.menu')}
            >
              <Menu />
            </Button>

            <div className="flex items-center gap-3">
              <div className="grid size-8 place-items-center rounded-md bg-primary/15 text-primary">
                <Activity className="size-4" />
              </div>
              <div className="leading-tight">
                <p className="text-body font-semibold text-foreground">{t('shell.brand')}</p>
                <p className="text-caption text-muted-foreground">{t('shell.brandSub')}</p>
              </div>
            </div>

            <span className="hidden items-center gap-1.5 rounded-full border border-success/30 bg-success/10 px-2.5 py-1 text-caption text-success sm:inline-flex">
              <span className="size-1.5 rounded-full bg-success" />
              {t('shell.env')} · {t('shell.cluster')}
            </span>

            <div className="ml-auto flex items-center gap-2">
              <Button variant="outline" size="icon" aria-label={t('shell.search')}>
                <Search />
              </Button>

              <button
                type="button"
                onClick={toggleLang}
                aria-label={`${t('shell.language')}: ${lang === 'pt' ? 'Português' : 'English'}`}
                className="inline-flex h-9 items-center rounded-md border border-border px-2.5 text-caption font-semibold text-surface-foreground transition-colors hover:bg-surface-muted"
              >
                {lang.toUpperCase()}
              </button>

              <span
                className={cn(
                  'hidden items-center gap-1.5 rounded-full border px-2.5 py-1 text-caption sm:inline-flex',
                  connected
                    ? 'border-success/30 bg-success/10 text-success'
                    : 'border-border bg-surface-muted text-muted-foreground',
                )}
                role="status"
                aria-label={connected ? t('shell.liveOn') : t('shell.liveOff')}
              >
                <span
                  className={cn(
                    'size-2 rounded-full',
                    connected ? 'bg-success animate-pulse-dot' : 'bg-muted-foreground',
                  )}
                />
                {connected ? t('shell.live') : t('shell.offline')}
              </span>

              <Button variant="outline" size="icon" onClick={toggle} aria-label={t('shell.theme')}>
                {theme === 'dark' ? <Sun /> : <Moon />}
              </Button>

              <button
                type="button"
                aria-label={t('shell.profile')}
                className="grid size-9 shrink-0 place-items-center rounded-full border border-border bg-surface-muted text-caption font-semibold text-surface-foreground"
              >
                MC
              </button>

              <CreateOrderDialog />
            </div>
          </div>
        </header>

        <main className="container flex-1 py-8">
          <Outlet />
        </main>

        <footer className="border-t border-border px-4 py-4 text-caption text-muted-foreground sm:px-6">
          {t('shell.footer')}
        </footer>
      </div>
    </div>
  );
}
