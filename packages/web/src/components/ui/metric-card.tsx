import { ArrowDownRight, ArrowUpRight, Minus } from 'lucide-react';
import { Card } from './card';
import { Sparkline } from './sparkline';
import { cn } from '@/lib/utils';

interface MetricCardProps {
  label: string;
  value: string | number;
  /** Signed percentage change vs. the previous window. */
  delta?: number;
  trend?: number[];
  tone?: 'primary' | 'success' | 'warning' | 'danger' | 'info';
  className?: string;
}

/** A KPI tile: value + directional delta + a sparkline of recent history. */
export function MetricCard({
  label,
  value,
  delta,
  trend,
  tone = 'primary',
  className,
}: MetricCardProps) {
  const direction = delta == null ? 'flat' : delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';
  const DeltaIcon = direction === 'up' ? ArrowUpRight : direction === 'down' ? ArrowDownRight : Minus;
  const deltaTone =
    direction === 'up' ? 'text-success' : direction === 'down' ? 'text-danger' : 'text-muted-foreground';

  return (
    <Card className={cn('p-5', className)}>
      <p className="text-caption uppercase tracking-wide text-muted-foreground">{label}</p>
      <div className="mt-2 flex items-end justify-between gap-3">
        <div>
          <p className="text-display leading-none text-surface-foreground">{value}</p>
          {delta != null && (
            <p className={cn('mt-2 flex items-center gap-1 text-caption font-medium', deltaTone)}>
              <DeltaIcon className="size-3.5" aria-hidden />
              {Math.abs(delta).toFixed(1)}%
            </p>
          )}
        </div>
        {trend && trend.length > 1 && <Sparkline data={trend} tone={tone} />}
      </div>
    </Card>
  );
}
