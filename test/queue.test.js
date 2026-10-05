'use strict';
const test = require('node:test');
const assert = require('node:assert');
const { JobQueue } = require('../queue');

const mk = (opts) => { const c = { t: 0 }; const q = new JobQueue({ now: () => c.t, ...opts }); return { q, c }; };

test('receive hides job until acked', () => {
  const { q } = mk();
  q.enqueue('a');
  const j = q.receive();
  assert.equal(q.receive(), null);
  assert.ok(q.ack(j.receipt));
  assert.equal(q.stats().pending, 0);
});

test('visibility timeout redelivers an unacked job', () => {
  const { q, c } = mk({ visibilityTimeoutMs: 100 });
  q.enqueue('a');
  const first = q.receive();
  c.t = 101;
  const second = q.receive();
  assert.equal(second.id, first.id);
  assert.equal(second.attempts, 2);
});

test('stale receipt cannot ack after redelivery', () => {
  const { q, c } = mk({ visibilityTimeoutMs: 100 });
  q.enqueue('a');
  const first = q.receive();
  c.t = 101;
  q.receive();
  assert.equal(q.ack(first.receipt), false);
});

test('nack applies exponential backoff', () => {
  const { q, c } = mk({ backoffBaseMs: 100 });
  q.enqueue('a');
  q.nack(q.receive().receipt);
  c.t = 99; assert.equal(q.receive(), null);
  c.t = 100; const j = q.receive(); assert.equal(j.attempts, 2);
  q.nack(j.receipt);
  c.t = 299; assert.equal(q.receive(), null);
  c.t = 300; assert.ok(q.receive());
});

test('exhausted attempts go to the DLQ', () => {
  const { q, c } = mk({ maxAttempts: 2, backoffBaseMs: 1 });
  q.enqueue('bad');
  q.nack(q.receive().receipt, 'boom');
  c.t = 10;
  q.nack(q.receive().receipt, 'boom');
  assert.deepEqual(q.stats(), { pending: 0, dead: 1 });
  assert.equal(q.dlq[0].reason, 'boom');
});

test('delayed enqueue is invisible until due', () => {
  const { q, c } = mk();
  q.enqueue('x', { delayMs: 50 });
  assert.equal(q.receive(), null);
  c.t = 50; assert.ok(q.receive());
});

test('repeated visibility timeouts also dead-letter', () => {
  const { q, c } = mk({ maxAttempts: 2, visibilityTimeoutMs: 10 });
  q.enqueue('hang');
  q.receive(); c.t = 11; q.receive(); c.t = 22;
  assert.equal(q.receive(), null);
  assert.equal(q.stats().dead, 1);
});
