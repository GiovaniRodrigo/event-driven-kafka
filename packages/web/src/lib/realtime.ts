import { io, type Socket } from 'socket.io-client';

let socket: Socket | null = null;

/**
 * Lazily-created Socket.IO singleton. In dev, Vite proxies `/socket.io` to the
 * backend; in prod the app is served from the backend origin, so the default
 * same-origin connection is correct. Override with VITE_SOCKET_URL if needed.
 */
export function getSocket(): Socket {
  if (!socket) {
    // Default transports (polling → upgrade) so the connection works both
    // directly and behind the dev proxy; forcing websocket-first fails to
    // upgrade through Vite's proxy.
    const url = import.meta.env.VITE_SOCKET_URL;
    socket = url ? io(url) : io();
  }
  return socket;
}
