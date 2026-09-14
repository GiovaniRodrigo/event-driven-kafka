import * as React from 'react';
import { mutate } from 'swr';
import { Plus, Trash2 } from 'lucide-react';
import { createOrderRequestSchema } from '@kafka-demo/contracts';
import { api, ApiError, swrKeys } from '@/lib/api';
import {
  Alert,
  Button,
  Input,
  Label,
  useToast,
} from '@/components/ui';
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from '@/components/ui/dialog';

interface ItemRow {
  name: string;
  price: string;
  quantity: string;
}

const EMPTY_ROW: ItemRow = { name: '', price: '', quantity: '1' };

/**
 * Create a test order to drive the pipeline during a demo. Validates against the
 * shared `createOrderRequestSchema` before POSTing; surfaces the API's 400 as
 * inline errors and its 202 as a success toast, then patches the orders cache.
 */
export function CreateOrderDialog() {
  const { toast } = useToast();
  const [open, setOpen] = React.useState(false);
  const [userId, setUserId] = React.useState('');
  const [rows, setRows] = React.useState<ItemRow[]>([{ ...EMPTY_ROW }]);
  const [error, setError] = React.useState<string | null>(null);
  const [submitting, setSubmitting] = React.useState(false);

  function reset() {
    setUserId('');
    setRows([{ ...EMPTY_ROW }]);
    setError(null);
  }

  function updateRow(index: number, patch: Partial<ItemRow>) {
    setRows((prev) => prev.map((r, i) => (i === index ? { ...r, ...patch } : r)));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    const candidate = {
      user_id: userId.trim(),
      items: rows
        .filter((r) => r.name.trim())
        .map((r, i) => ({
          sku: `SKU-${i + 1}`,
          name: r.name.trim(),
          price: Number(r.price),
          quantity: Number(r.quantity),
        })),
    };

    const parsed = createOrderRequestSchema.safeParse(candidate);
    if (!parsed.success) {
      setError('Add a user id and at least one item with a name, price, and quantity.');
      return;
    }

    setSubmitting(true);
    try {
      const res = await api.createOrder(parsed.data);
      toast({ tone: 'success', title: 'Order accepted', description: `202 · ${res.order_id}` });
      void mutate(swrKeys.orders);
      reset();
      setOpen(false);
    } catch (err) {
      if (err instanceof ApiError && err.status === 400) {
        setError('The API rejected the order (400): user_id and items are required.');
      } else {
        toast({ tone: 'danger', title: 'Could not create order', description: 'The API is unreachable.' });
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) reset();
      }}
    >
      <DialogTrigger asChild>
        <Button>
          <Plus /> New order
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Create a test order</DialogTitle>
          <DialogDescription>Triggers the Kafka pipeline end to end.</DialogDescription>
        </DialogHeader>

        <form className="space-y-4" onSubmit={handleSubmit}>
          <div className="space-y-1.5">
            <Label htmlFor="user_id">User id</Label>
            <Input
              id="user_id"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
              placeholder="user-123"
              invalid={Boolean(error) && !userId.trim()}
            />
          </div>

          <div className="space-y-2">
            <Label>Items</Label>
            {rows.map((row, i) => (
              <div key={i} className="flex gap-2">
                <Input
                  aria-label={`Item ${i + 1} name`}
                  value={row.name}
                  onChange={(e) => updateRow(i, { name: e.target.value })}
                  placeholder="Widget"
                  className="flex-1"
                />
                <Input
                  aria-label={`Item ${i + 1} price`}
                  value={row.price}
                  onChange={(e) => updateRow(i, { price: e.target.value })}
                  placeholder="9.99"
                  inputMode="decimal"
                  className="w-24"
                />
                <Input
                  aria-label={`Item ${i + 1} quantity`}
                  value={row.quantity}
                  onChange={(e) => updateRow(i, { quantity: e.target.value })}
                  inputMode="numeric"
                  className="w-16"
                />
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label={`Remove item ${i + 1}`}
                  disabled={rows.length === 1}
                  onClick={() => setRows((prev) => prev.filter((_, idx) => idx !== i))}
                >
                  <Trash2 />
                </Button>
              </div>
            ))}
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => setRows((prev) => [...prev, { ...EMPTY_ROW }])}
            >
              <Plus /> Add item
            </Button>
          </div>

          {error && (
            <Alert tone="danger" title="Check the form">
              {error}
            </Alert>
          )}

          <div className="flex justify-end gap-2 pt-2">
            <DialogClose asChild>
              <Button type="button" variant="ghost">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={submitting}>
              {submitting ? 'Submitting…' : 'Submit order'}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  );
}
