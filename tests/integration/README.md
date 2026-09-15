# Integration tests

Reserved for tests that exercise the real infrastructure (Kafka, Postgres,
Socket.IO) started via `npm run docker:up` and `npm run db:migrate`.

Current suites under `tests/unit/` are fully mocked. Add integration specs here
as `*.test.ts`; `npm run test:integration` will pick them up automatically.
