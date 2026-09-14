import type { OrderStatus } from '@kafka-demo/contracts';
import { Badge } from './badge';
import { ORDER_STATUS_META } from '@/lib/status';
import { cn } from '@/lib/utils';

interface StatusBadgeProps {
  status: OrderStatus;
  className?: string;
}

/** Renders an order status as a semantic, scannable badge. */
export function StatusBadge({ status, className }: StatusBadgeProps) {
  const meta = ORDER_STATUS_META[status];
  return (
    <Badge tone={meta.tone} className={cn('capitalize', className)}>
      {meta.label}
    </Badge>
  );
}
