'use strict';
// Worker pool: N concurrent loops pulling from the queue.
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runWorkers(queue, handler, { concurrency = 3, pollMs = 20, idleExitMs = 300 } = {}) {
  let lastWork = Date.now();
  const loop = async (n) => {
    while (Date.now() - lastWork < idleExitMs || queue.stats().pending > 0) {
      const job = queue.receive();
      if (!job) { await sleep(pollMs); if (queue.stats().pending === 0 && Date.now() - lastWork >= idleExitMs) break; continue; }
      lastWork = Date.now();
      try { await handler(job.payload, job); queue.ack(job.receipt); }
      catch (err) { queue.nack(job.receipt, err.message); }
      lastWork = Date.now();
    }
  };
  await Promise.all(Array.from({ length: concurrency }, (_, i) => loop(i)));
}

module.exports = { runWorkers };
