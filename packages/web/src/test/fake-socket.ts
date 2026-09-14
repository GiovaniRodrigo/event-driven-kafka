/**
 * Minimal in-memory stand-in for the Socket.IO client used in tests. Views
 * register handlers via `on`; a test drives the "server" side with
 * `emitServer(event, payload)`. `getSocket` is mocked to return this instance.
 */
type Handler = (payload: unknown) => void;

class FakeSocket {
  connected = true;
  private handlers = new Map<string, Set<Handler>>();
  /** Records client→server emits (e.g. subscribe/unsubscribe) for assertions. */
  public sent: Array<{ event: string; payload: unknown }> = [];

  on(event: string, handler: Handler): this {
    if (!this.handlers.has(event)) this.handlers.set(event, new Set());
    this.handlers.get(event)!.add(handler);
    return this;
  }

  off(event: string, handler: Handler): this {
    this.handlers.get(event)?.delete(handler);
    return this;
  }

  emit(event: string, payload?: unknown): this {
    this.sent.push({ event, payload });
    return this;
  }

  /** Test helper: deliver a server-sent event to registered handlers. */
  emitServer(event: string, payload: unknown): void {
    this.handlers.get(event)?.forEach((h) => h(payload));
  }

  reset(): void {
    this.handlers.clear();
    this.sent = [];
    this.connected = true;
  }
}

export const fakeSocket = new FakeSocket();
