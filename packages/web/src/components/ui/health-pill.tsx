import type { ConsumerHealth } from '@kafka-demo/contracts';
import { CONSUMER_HEALTH_META } from '@/lib/status';
import { cn } from '@/lib/utils';

const DOT_TONE: Record<ConsumerHealth, string> = {
  healthy: 'bg-success',
  degraded: 'bg-warning',
  down: 'bg-danger',
};

interface HealthPillProps {
  health: ConsumerHealth;
  label?: string;
  className?: string;
}

/**
 * A consumer's health as a labelled status dot. The dot pulses while degraded
 * so a stuck part of the pipeline draws the eye without relying on color alone.
 */
export function HealthPill({ health, label, className }: HealthPillProps) {
  const meta = CONSUMER_HEALTH_META[health];
  return (
    <span
      className={cn(
        'inline-flex items-center gap-2 rounded-full border border-border bg-surface px-3 py-1 text-body',
        className,
      )}
      role="status"
      aria-label={`${label ? `${label}: ` : ''}${meta.label}`}
    >
      <span
        className={cn(
          'size-2 rounded-full',
          DOT_TONE[health],
          health !== 'healthy' && 'animate-pulse-dot',
        )}
      />
      {label && <span className="font-medium text-surface-foreground">{label}</span>}
      <span className="text-muted-foreground">{meta.label}</span>
    </span>
  );
}
