import type * as React from 'react';
import { cn } from '@/lib/utils';

/** Loading placeholder — distinguishes "loading" from "empty". */
export function Skeleton({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn('animate-pulse rounded-md bg-surface-muted', className)}
      aria-hidden
      {...props}
    />
  );
}
