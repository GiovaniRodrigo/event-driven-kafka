/**
 * `@kafka-demo/contracts` — the single, runtime-validated source of truth for
 * every payload crossing a boundary in this system: the REST responses the
 * backend returns and the Socket.IO messages the read-side bridge broadcasts.
 * Both the backend and the web dashboard import these zod schemas and infer
 * their TypeScript types from them, so a contract drift surfaces as a boundary
 * (parse) failure on either side rather than a silent shape mismatch.
 */
export * from './events';
export * from './rest';
