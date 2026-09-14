import { NavLink, Outlet } from 'react-router-dom';
import { Activity, LayoutDashboard, Moon, Sun } from 'lucide-react';
import { useTheme } from '@/components/theme-provider';
import { useRealtimeSync } from '@/hooks/use-realtime-sync';
import { CreateOrderDialog } from '@/components/create-order-dialog';
import { Button } from '@/components/ui';
import { cn } from '@/lib/utils';

const NAV = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, end: true },
  { to: '/consumers', label: 'Consumers', icon: Activity, end: false },
];

/** App shell: top bar with nav, live-connection indicator, new-order, theme. */
export function AppLayout() {
  const { theme, toggle } = useTheme();
  const { connected } = useRealtimeSync();

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-40 border-b border-border bg-background/80 backdrop-blur">
        <div className="container flex flex-wrap items-center gap-4 py-3">
          <div className="flex items-center gap-2">
            <span className="text-heading font-semibold text-foreground">Kafka Orders</span>
            <span
              className={cn(
                'inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-caption',
                connected
                  ? 'border-success/30 bg-success/10 text-success'
                  : 'border-muted bg-surface-muted text-muted-foreground',
              )}
              role="status"
              aria-label={connected ? 'Live connection active' : 'Live connection offline'}
            >
              <span className={cn('size-2 rounded-full', connected ? 'bg-success animate-pulse-dot' : 'bg-muted-foreground')} />
              {connected ? 'Live' : 'Offline'}
            </span>
          </div>

          <nav className="flex items-center gap-1">
            {NAV.map(({ to, label, icon: Icon, end }) => (
              <NavLink
                key={to}
                to={to}
                end={end}
                className={({ isActive }) =>
                  cn(
                    'inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-body font-medium transition-colors',
                    isActive
                      ? 'bg-surface-muted text-surface-foreground'
                      : 'text-muted-foreground hover:bg-surface-muted hover:text-surface-foreground',
                  )
                }
              >
                <Icon className="size-4" /> {label}
              </NavLink>
            ))}
          </nav>

          <div className="ml-auto flex items-center gap-2">
            <CreateOrderDialog />
            <Button variant="outline" size="icon" onClick={toggle} aria-label="Toggle theme">
              {theme === 'dark' ? <Sun /> : <Moon />}
            </Button>
          </div>
        </div>
      </header>

      <main className="container py-8">
        <Outlet />
      </main>
    </div>
  );
}
