'use strict';
const { JobQueue } = require('./queue');
const { runWorkers } = require('./worker');

const q = new JobQueue({ maxAttempts: 3, backoffBaseMs: 50, visibilityTimeoutMs: 1000 });
for (let i = 1; i <= 8; i++) q.enqueue({ orderId: i });
q.enqueue({ orderId: 'poison' });

const seen = {};
runWorkers(q, async (p, job) => {
  seen[p.orderId] = (seen[p.orderId] || 0) + 1;
  if (p.orderId === 'poison') throw new Error('cannot parse order');
  if (p.orderId === 3 && job.attempts < 2) throw new Error('transient upstream 503');
  console.log(`processed order ${p.orderId} (attempt ${job.attempts})`);
}).then(() => {
  console.log('\nstats:', q.stats());
  console.log('dead-letter queue:', q.dlq);
});
