import { useNavigate } from 'react-router-dom';
import { useHealth, useMetrics, useOrders } from '@/hooks/use-dashboard-data';
import {
  Alert,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  HealthPill,
  MetricCard,
  OrdersTable,
  Skeleton,
} from '@/components/ui';
import { formatCurrency, toNumber } from '@/lib/utils';

const CONSUMERS = ['payment', 'inventory', 'notification'] as const;

/** Operational at-a-glance: KPI row, recent orders, consumer health — all live. */
export function Overview() {
  const navigate = useNavigate();
  const metrics = useMetrics();
  const orders = useOrders();
  const health = useHealth();

  return (
    <div className="space-y-8">
      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {metrics.isLoading && !metrics.data ? (
          Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-28" />)
        ) : metrics.error ? (
          <Alert tone="danger" title="Metrics unavailable" className="sm:col-span-2 lg:col-span-4">
            The backend is unreachable.
          </Alert>
        ) : (
          <>
            <MetricCard label="Total orders" value={toNumber(metrics.data?.total_orders).toLocaleString()} tone="primary" />
            <MetricCard label="Completed" value={toNumber(metrics.data?.completed_orders).toLocaleString()} tone="success" />
            <MetricCard label="Failed" value={toNumber(metrics.data?.failed_orders).toLocaleString()} tone="danger" />
            <MetricCard label="Avg order value" value={formatCurrency(toNumber(metrics.data?.avg_order_value))} tone="info" />
          </>
        )}
      </section>

      <section className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Recent orders</CardTitle>
          </CardHeader>
          <CardContent>
            {orders.isLoading && !orders.data ? (
              <div className="space-y-2">
                {Array.from({ length: 4 }).map((_, i) => (
                  <Skeleton key={i} className="h-12" />
                ))}
              </div>
            ) : orders.error ? (
              <Alert tone="danger" title="Orders unavailable">
                Could not reach the backend. Check that the API is running.
              </Alert>
            ) : (
              <OrdersTable
                orders={orders.data?.orders ?? []}
                onSelect={(id) => navigate(`/orders/${id}`)}
              />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Consumer health</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            {health.isLoading && !health.data ? (
              Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-9" />)
            ) : health.error ? (
              <Alert tone="danger" title="Health unavailable">
                The backend is unreachable.
              </Alert>
            ) : (
              CONSUMERS.map((name) => (
                <HealthPill
                  key={name}
                  label={name[0].toUpperCase() + name.slice(1)}
                  health={health.data?.consumers[name] ?? 'down'}
                />
              ))
            )}
          </CardContent>
        </Card>
      </section>
    </div>
  );
}
