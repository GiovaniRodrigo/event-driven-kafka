import type { OrderListItem } from '@kafka-demo/contracts';
import { StatusBadge } from './status-badge';
import { cn, formatCurrency, formatTime, shortId } from '@/lib/utils';

interface OrdersTableProps {
  orders: OrderListItem[];
  onSelect?: (orderId: string) => void;
  className?: string;
}

/**
 * Recent orders. Order ids render in mono so they read as identifiers; failed
 * rows are tinted so unhappy paths are as legible as happy ones. Presentational
 * — the parent supplies rows and handles selection.
 */
export function OrdersTable({ orders, onSelect, className }: OrdersTableProps) {
  if (orders.length === 0) {
    return (
      <div className={cn('rounded-lg border border-dashed border-border p-8 text-center', className)}>
        <p className="text-body text-muted-foreground">No orders yet.</p>
      </div>
    );
  }

  return (
    <div className={cn('overflow-x-auto rounded-lg border border-border', className)}>
      <table className="w-full min-w-[560px] border-collapse text-body">
        <thead>
          <tr className="border-b border-border bg-surface-muted text-left text-caption uppercase tracking-wide text-muted-foreground">
            <th className="px-4 py-2.5 font-medium">Order</th>
            <th className="px-4 py-2.5 font-medium">User</th>
            <th className="px-4 py-2.5 font-medium">Status</th>
            <th className="px-4 py-2.5 text-right font-medium">Total</th>
            <th className="px-4 py-2.5 font-medium">Created</th>
          </tr>
        </thead>
        <tbody>
          {orders.map((order) => {
            const failed = order.status === 'failed';
            const selectable = Boolean(onSelect);
            return (
              <tr
                key={order.order_id}
                onClick={selectable ? () => onSelect?.(order.order_id) : undefined}
                onKeyDown={
                  selectable
                    ? (e) => {
                        if (e.key === 'Enter' || e.key === ' ') {
                          e.preventDefault();
                          onSelect?.(order.order_id);
                        }
                      }
                    : undefined
                }
                tabIndex={selectable ? 0 : undefined}
                role={selectable ? 'button' : undefined}
                aria-label={selectable ? `Open order ${order.order_id}` : undefined}
                className={cn(
                  'border-b border-border/60 last:border-0 transition-colors',
                  selectable && 'cursor-pointer hover:bg-surface-muted',
                  failed && 'bg-danger/5',
                )}
              >
                <td className="px-4 py-3">
                  <code className="font-mono text-caption text-surface-foreground">
                    {shortId(order.order_id)}
                  </code>
                </td>
                <td className="px-4 py-3 text-muted-foreground">{order.user_id}</td>
                <td className="px-4 py-3">
                  <StatusBadge status={order.status} />
                </td>
                <td className="px-4 py-3 text-right tabular-nums text-surface-foreground">
                  {formatCurrency(order.total_amount)}
                </td>
                <td className="px-4 py-3 text-caption text-muted-foreground">
                  {formatTime(order.created_at)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
