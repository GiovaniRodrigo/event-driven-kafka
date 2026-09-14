interface SparklineProps {
  data: number[];
  className?: string;
  width?: number;
  height?: number;
  /** Semantic stroke color via a CSS variable name, e.g. 'success'. */
  tone?: 'primary' | 'success' | 'warning' | 'danger' | 'info';
}

/**
 * Minimal inline-SVG sparkline — no charting dependency. Renders a normalized
 * polyline of the series so a MetricCard can show direction, not just a number.
 */
export function Sparkline({
  data,
  className,
  width = 96,
  height = 28,
  tone = 'primary',
}: SparklineProps) {
  if (data.length < 2) {
    return <svg width={width} height={height} className={className} aria-hidden />;
  }

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const step = width / (data.length - 1);

  const points = data
    .map((v, i) => {
      const x = i * step;
      const y = height - ((v - min) / range) * (height - 4) - 2;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');

  return (
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={className}
      fill="none"
      role="img"
      aria-label="trend"
    >
      <polyline
        points={points}
        stroke={`hsl(var(--${tone}))`}
        strokeWidth={1.75}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
