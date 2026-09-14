import { useNavigate } from 'react-router-dom';
import { useI18n } from '@/i18n';
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
  const { t } = useI18n();
  const metrics = useMetrics();
  const orders = useOrders();
  const health = useHealth();

  return (
    <div className="space-y-8">
      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {metrics.isLoading && !metrics.data ? (
          Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-28" />)
        ) : metrics.error ? (
          <Alert tone="danger" title={t('error.metrics')} className="sm:col-span-2 lg:col-span-4">
            {t('error.backendUnreachable')}
          </Alert>
        ) : (
          <>
            <MetricCard label={t('metric.total')} value={toNumber(metrics.data?.total_orders).toLocaleString()} tone="primary" />
            <MetricCard label={t('metric.completed')} value={toNumber(metrics.data?.completed_orders).toLocaleString()} tone="success" />
            <MetricCard label={t('metric.failed')} value={toNumber(metrics.data?.failed_orders).toLocaleString()} tone="danger" />
            <MetricCard label={t('metric.avg')} value={formatCurrency(toNumber(metrics.data?.avg_order_value))} tone="info" />
          </>
        )}
      </section>

      <section className="grid gap-6 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>{t('overview.recentOrders')}</CardTitle>
          </CardHeader>
          <CardContent>
            {orders.isLoading && !orders.data ? (
              <div className="space-y-2">
                {Array.from({ length: 4 }).map((_, i) => (
                  <Skeleton key={i} className="h-12" />
                ))}
              </div>
            ) : orders.error ? (
              <Alert tone="danger" title={t('error.orders')}>
                {t('error.ordersBody')}
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
            <CardTitle>{t('overview.consumerHealth')}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            {health.isLoading && !health.data ? (
              Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-9" />)
            ) : health.error ? (
              <Alert tone="danger" title={t('error.health')}>
                {t('error.backendUnreachable')}
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
