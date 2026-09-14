import * as React from 'react';
import { cn } from '@/lib/utils';

export type InputProps = React.InputHTMLAttributes<HTMLInputElement> & {
  invalid?: boolean;
};

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ className, invalid, ...props }, ref) => (
    <input
      ref={ref}
      aria-invalid={invalid || undefined}
      className={cn(
        'h-9 w-full rounded-md border bg-surface px-3 text-body text-surface-foreground placeholder:text-muted-foreground',
        'border-input transition-colors focus-visible:border-ring',
        'disabled:cursor-not-allowed disabled:opacity-50',
        invalid && 'border-danger focus-visible:border-danger focus-visible:ring-danger',
        className,
      )}
      {...props}
    />
  ),
);
Input.displayName = 'Input';
