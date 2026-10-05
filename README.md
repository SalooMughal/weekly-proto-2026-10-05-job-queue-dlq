# Job Queue with Visibility Timeout, Retries & Dead-Letter Queue

> Small standalone demo built to explore SQS-style at-least-once queue semantics in plain Node.js. It is a prototype for learning/portfolio purposes — not used in production anywhere.

## What it does

An in-memory job queue plus a concurrent worker pool:

- **Visibility timeout** – a received job is hidden; if the worker crashes or hangs, it reappears and is redelivered.
- **Receipt handles** – each delivery gets a unique receipt, so a slow worker whose job was already redelivered can't ack it (stale ack is rejected).
- **Retries with exponential backoff** – `nack` re-schedules the job after `base * 2^(attempt-1)` ms.
- **Dead-letter queue** – jobs exceeding `maxAttempts` (via failures *or* repeated timeouts) are parked in the DLQ with a reason.
- **Delayed jobs** and an **injectable clock**, so every time-based behavior is tested deterministically (no sleeps in tests).

## Run it

Requires Node 18+ (no dependencies).

```bash
npm test      # 7 node:test tests
npm run demo  # 9 orders, one flaky (succeeds on retry), one poison (lands in DLQ)
```

## Files

- `queue.js` – the queue (state, receipts, backoff, DLQ)
- `worker.js` – concurrent worker loop pool
- `demo.js` – runnable scenario
- `test/queue.test.js` – deterministic tests using a fake clock

## What it demonstrates

Understanding of at-least-once delivery, why handlers must be idempotent, failure isolation via DLQs, and designing time-dependent logic for testability. Limitations: in-memory only, single process, O(n) receive scan.
