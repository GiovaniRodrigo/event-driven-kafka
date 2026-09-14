import { useHealth } from '@/hooks/use-dashboard-data';
import {
  Alert,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  HealthPill,
  Skeleton,
} from '@/components/ui';
import { cn } from '@/lib/utils';

const CONSUMERS = ['payment', 'inventory', 'notification'] as const;

/** Consumer health board — spot a stuck stage at a glance (updates live). */
export function Consumers() {
  const { data, error, isLoading } = useHealth();

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <div>
        <h1 className="text-title text-foreground">Consumers</h1>
        <p className="text-body text-muted-foreground">
          Health of each pipeline stage, broadcast live over Socket.IO.
        </p>
      </div>

      {isLoading && !data ? (
        <div className="grid gap-4 sm:grid-cols-3">
          {CONSUMERS.map((c) => (
            <Skeleton key={c} className="h-28" />
          ))}
        </div>
      ) : error ? (
        <Alert tone="danger" title="Health unavailable">
          The backend is unreachable.
        </Alert>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-3">
            {CONSUMERS.map((name) => {
              const status = data?.consumers[name] ?? 'down';
              return (
                <Card key={name}>
                  <CardHeader>
                    <CardTitle className="capitalize">{name}</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <HealthPill health={status} />
                  </CardContent>
                </Card>
              );
            })}
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Service</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-wrap items-center gap-x-8 gap-y-2 text-body">
              <span>
                <span className="text-muted-foreground">Status: </span>
                <span
                  className={cn(
                    'font-medium',
                    data?.status === 'ok' ? 'text-success' : 'text-warning',
                  )}
                >
                  {data?.status ?? 'unknown'}
                </span>
              </span>
              <span>
                <span className="text-muted-foreground">Database: </span>
                <span
                  className={cn(
                    'font-medium',
                    data?.database === 'healthy' ? 'text-success' : 'text-danger',
                  )}
                >
                  {data?.database ?? 'unknown'}
                </span>
              </span>
              <span className="text-muted-foreground">v{data?.version ?? '—'}</span>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
