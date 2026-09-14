import type { OrderEventPayload, OrderListItem } from '@kafka-demo/contracts';
import { Moon, Sun } from 'lucide-react';
import { useTheme } from '@/components/theme-provider';
import {
  Alert,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  EventTimeline,
  HealthPill,
  MetricCard,
  OrdersTable,
  StatusBadge,
  useToast,
} from '@/components/ui';

/**
 * Phase-2 showcase: a single page that exercises the design system with mock
 * data so the tokens and components can be reviewed together. Phase 4 replaces
 * this with the routed, live-data dashboard (this file becomes the router).
 */

const MOCK_ORDERS: OrderListItem[] = [
  { order_id: 'a1b2c3d4e5f60718', user_id: 'user-204', status: 'completed', total_amount: 149.9, created_at: new Date().toISOString() },
  { order_id: 'b2c3d4e5f6071829', user_id: 'user-118', status: 'inventory_reserved', total_amount: 89.5, created_at: new Date().toISOString() },
  { order_id: 'c3d4e5f607182930', user_id: 'user-771', status: 'payment_processing', total_amount: 320.0, created_at: new Date().toISOString() },
  { order_id: 'd4e5f60718293041', user_id: 'user-052', status: 'failed', total_amount: 42.25, created_at: new Date().toISOString() },
];

const MOCK_EVENTS: OrderEventPayload[] = [
  { event_type: 'order.created', topic: 'orders', timestamp: new Date(Date.now() - 40000).toISOString() },
  { event_type: 'payment.processed', topic: 'payments', timestamp: new Date(Date.now() - 25000).toISOString() },
  { event_type: 'inventory.reserved', topic: 'inventory', timestamp: new Date(Date.now() - 8000).toISOString() },
];

export function App() {
  const { theme, toggle } = useTheme();
  const { toast } = useToast();

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b border-border">
        <div className="container flex items-center justify-between py-4">
          <div>
            <h1 className="text-title text-foreground">Kafka Order Dashboard</h1>
            <p className="text-body text-muted-foreground">Design system showcase</p>
          </div>
          <Button variant="outline" size="icon" onClick={toggle} aria-label="Toggle theme">
            {theme === 'dark' ? <Sun /> : <Moon />}
          </Button>
        </div>
      </header>

      <main className="container space-y-8 py-8">
        <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
          <MetricCard label="Orders / min" value="24" delta={12.4} trend={[3, 5, 4, 8, 7, 10, 9, 12]} tone="primary" />
          <MetricCard label="Events processed" value="1,208" delta={4.1} trend={[40, 42, 48, 47, 55, 60, 58, 64]} tone="info" />
          <MetricCard label="Completed" value="312" delta={0} tone="success" />
          <MetricCard label="Consumer lag" value="18" delta={-6.2} trend={[30, 28, 26, 24, 22, 20, 19, 18]} tone="warning" />
        </section>

        <section className="grid gap-6 lg:grid-cols-3">
          <Card className="lg:col-span-2">
            <CardHeader>
              <CardTitle>Recent orders</CardTitle>
            </CardHeader>
            <CardContent>
              <OrdersTable orders={MOCK_ORDERS} onSelect={(id) => toast({ tone: 'info', title: 'Open order', description: id })} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Consumer health</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              <HealthPill label="Payment" health="healthy" />
              <HealthPill label="Inventory" health="degraded" />
              <HealthPill label="Notification" health="down" />
            </CardContent>
          </Card>
        </section>

        <section className="grid gap-6 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Event timeline</CardTitle>
            </CardHeader>
            <CardContent>
              <EventTimeline events={MOCK_EVENTS} />
            </CardContent>
          </Card>

          <div className="space-y-4">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge status="pending" />
              <StatusBadge status="payment_approved" />
              <StatusBadge status="completed" />
              <StatusBadge status="failed" />
            </div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={() => toast({ tone: 'success', title: 'Order accepted', description: '202 Accepted' })}>Success toast</Button>
              <Button variant="secondary">Secondary</Button>
              <Button variant="outline">Outline</Button>
              <Button variant="danger" onClick={() => toast({ tone: 'danger', title: 'Request failed' })}>Danger</Button>
            </div>
            <Alert tone="warning" title="Heads up">
              This is a static showcase; wire it to the backend in phase 4.
            </Alert>
          </div>
        </section>
      </main>
    </div>
  );
}
