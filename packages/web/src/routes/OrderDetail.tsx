import { Link, useParams } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';
import { useI18n } from '@/i18n';
import { useOrder } from '@/hooks/use-dashboard-data';
import { useOrderRealtime } from '@/hooks/use-order-realtime';
import { ApiError } from '@/lib/api';
import {
  Alert,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  EventTimeline,
  Skeleton,
  StatusBadge,
} from '@/components/ui';
import { formatCurrency, formatTime } from '@/lib/utils';

/** A single order: summary, line items, and its live event timeline. */
export function OrderDetail() {
  const { id } = useParams<{ id: string }>();
  const { t } = useI18n();
  const { data, error, isLoading } = useOrder(id);
  useOrderRealtime(id);

  const notFound = error instanceof ApiError && error.status === 404;

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <Link
        to="/"
        className="inline-flex items-center gap-1.5 text-body text-muted-foreground hover:text-foreground"
      >
        <ArrowLeft className="size-4" /> {t('order.back')}
      </Link>

      {isLoading && !data ? (
        <div className="space-y-4">
          <Skeleton className="h-24" />
          <Skeleton className="h-64" />
        </div>
      ) : notFound ? (
        <Alert tone="warning" title={t('order.notFound')}>
          {t('order.notFoundBody')} <code className="font-mono">{id}</code>.
        </Alert>
      ) : error ? (
        <Alert tone="danger" title={t('order.loadError')}>
          {t('error.backendUnreachable')}
        </Alert>
      ) : data ? (
        <>
          <Card>
            <CardHeader className="flex-row items-start justify-between">
              <div className="space-y-1">
                <p className="text-caption uppercase tracking-wide text-muted-foreground">{t('order.order')}</p>
                <code className="font-mono text-heading text-surface-foreground">{data.order_id}</code>
                <p className="text-body text-muted-foreground">
                  {data.user_id} · {t('order.created')} {formatTime(data.created_at)}
                </p>
              </div>
              <StatusBadge status={data.status} />
            </CardHeader>
            <CardContent className="flex flex-wrap gap-x-8 gap-y-2">
              <div>
                <p className="text-caption uppercase tracking-wide text-muted-foreground">{t('order.total')}</p>
                <p className="text-heading tabular-nums text-surface-foreground">
                  {formatCurrency(data.total_amount)}
                </p>
              </div>
              <div className="min-w-[12rem] flex-1">
                <p className="text-caption uppercase tracking-wide text-muted-foreground">{t('order.items')}</p>
                <ul className="mt-1 space-y-1 text-body">
                  {data.items.length === 0 && <li className="text-muted-foreground">{t('order.noItems')}</li>}
                  {data.items.map((item, i) => (
                    <li key={`${item.sku}-${i}`} className="flex justify-between gap-4">
                      <span className="text-surface-foreground">
                        {item.quantity}× {item.name}
                      </span>
                      <span className="tabular-nums text-muted-foreground">
                        {formatCurrency(item.price * item.quantity)}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>{t('order.timeline')}</CardTitle>
            </CardHeader>
            <CardContent>
              <EventTimeline events={data.events} failed={data.status === 'failed'} />
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  );
}
