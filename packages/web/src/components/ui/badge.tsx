import * as React from 'react';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from '@/lib/utils';

/**
 * Low-level badge. `tone` maps 1:1 to the semantic color tokens; higher-level
 * components (StatusBadge, HealthPill) choose the tone from domain state.
 */
const badgeVariants = cva(
  'inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border px-2.5 py-0.5 text-caption font-medium',
  {
    variants: {
      tone: {
        neutral: 'border-border bg-surface-muted text-muted-foreground',
        info: 'border-info/30 bg-info/15 text-info',
        primary: 'border-primary/30 bg-primary/15 text-primary',
        success: 'border-success/30 bg-success/15 text-success',
        warning: 'border-warning/30 bg-warning/15 text-warning',
        danger: 'border-danger/30 bg-danger/15 text-danger',
      },
    },
    defaultVariants: { tone: 'neutral' },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, tone, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ tone }), className)} {...props} />;
}

export { badgeVariants };
