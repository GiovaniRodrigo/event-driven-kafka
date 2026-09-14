import type { OrderEventPayload } from '@kafka-demo/contracts';
import { cn } from '@/lib/utils';
import { formatTime } from '@/lib/utils';

interface EventTimelineProps {
  events: OrderEventPayload[];
  /** When the order failed, the last node is marked as a failure. */
  failed?: boolean;
  className?: string;
}

/**
 * An order's journey through the Kafka topics: one node per recorded event,
 * newest last, with the topic in mono type and a local timestamp. Renders an
 * empty-state line when no events have arrived yet.
 */
export function EventTimeline({ events, failed = false, className }: EventTimelineProps) {
  if (events.length === 0) {
    return (
      <p className={cn('text-body text-muted-foreground', className)}>
        No pipeline events recorded yet.
      </p>
    );
  }

  return (
    <ol className={cn('relative flex flex-col gap-0', className)}>
      {events.map((event, i) => {
        const isLast = i === events.length - 1;
        const isFailureNode = failed && isLast;
        return (
          <li key={`${event.event_type}-${event.timestamp}-${i}`} className="flex gap-3">
            <div className="flex flex-col items-center">
              <span
                className={cn(
                  'mt-1 size-2.5 shrink-0 rounded-full ring-4 ring-surface',
                  isFailureNode ? 'bg-danger' : 'bg-primary',
                )}
              />
              {!isLast && <span className="w-px flex-1 bg-border" />}
            </div>
            <div className={cn('pb-5', isLast && 'pb-0')}>
              <p className="text-body font-medium text-surface-foreground">
                {formatEventType(event.event_type)}
              </p>
              <p className="mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-caption text-muted-foreground">
                <code className="rounded bg-surface-muted px-1.5 py-0.5 font-mono text-[0.7rem]">
                  {event.topic}
                </code>
                <span>{formatTime(event.timestamp)}</span>
              </p>
            </div>
          </li>
        );
      })}
    </ol>
  );
}

function formatEventType(type: string): string {
  return type
    .replace(/[._]/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}
