'use strict';
// In-memory job queue with visibility timeout, retries w/ backoff, and a dead-letter queue.
// Semantics mirror SQS-style at-least-once delivery. Clock is injectable for deterministic tests.

class JobQueue {
  constructor({ visibilityTimeoutMs = 30000, maxAttempts = 3, backoffBaseMs = 100, now = () => Date.now() } = {}) {
    Object.assign(this, { visibilityTimeoutMs, maxAttempts, backoffBaseMs, now });
    this.jobs = new Map(); // id -> job
    this.dlq = [];
    this.seq = 0;
  }

  enqueue(payload, { delayMs = 0 } = {}) {
    const id = `job-${++this.seq}`;
    this.jobs.set(id, { id, payload, attempts: 0, visibleAt: this.now() + delayMs, receipt: null });
    return id;
  }

  // Claim the next visible job. It becomes invisible until acked, nacked, or the timeout expires.
  receive() {
    const t = this.now();
    for (const job of this.jobs.values()) {
      if (job.visibleAt <= t) {
        job.attempts++;
        if (job.attempts > this.maxAttempts) { this._deadLetter(job, 'max attempts exceeded (visibility timeouts)'); continue; }
        job.visibleAt = t + this.visibilityTimeoutMs;
        job.receipt = `${job.id}:${job.attempts}`;
        return { id: job.id, payload: job.payload, attempts: job.attempts, receipt: job.receipt };
      }
    }
    return null;
  }

  ack(receipt) {
    const job = this._byReceipt(receipt);
    if (!job) return false; // stale receipt (timed out and redelivered)
    this.jobs.delete(job.id);
    return true;
  }

  nack(receipt, reason = 'failed') {
    const job = this._byReceipt(receipt);
    if (!job) return false;
    if (job.attempts >= this.maxAttempts) { this._deadLetter(job, reason); return true; }
    job.visibleAt = this.now() + this.backoffBaseMs * 2 ** (job.attempts - 1);
    job.receipt = null;
    return true;
  }

  _byReceipt(receipt) {
    const id = receipt && receipt.split(':')[0];
    const job = this.jobs.get(id);
    return job && job.receipt === receipt ? job : null;
  }

  _deadLetter(job, reason) {
    this.jobs.delete(job.id);
    this.dlq.push({ id: job.id, payload: job.payload, attempts: job.attempts, reason });
  }

  stats() {
    return { pending: this.jobs.size, dead: this.dlq.length };
  }
}

module.exports = { JobQueue };
