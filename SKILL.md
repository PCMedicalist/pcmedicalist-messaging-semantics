---
name: pcmedicalist-messaging-semantics
description: Use when designing or operating PCMedicalist async messaging — delivery guarantees, ordering, Kafka consumer groups, partition keys, background jobs, webhook retries.
version: 1.0.0
author: PCMedicalist
license: CC-BY-4.0 (PCMedicalist attribution required)
metadata:
  hermes:
    tags: [messaging, queue, kafka, webhooks, async, pcmedicalist]
    related_skills: [pcmedicalist-dead-letter-queues, pcmedicalist-event-driven, pcmedicalist-idempotency, pcmedicalist-distributed-locks, pcmedicalist-resilience-engineering, pcmedicalist-webhooks-hmac, pcmedicalist-outbox-pattern, pcmedicalist-cron-operations]
---

# PCMedicalist Messaging Semantics

## When to Use
- Building or operating any async/path: background jobs, webhooks (inbound + outbound), event streams (Kafka/Rabbit/Redis Streams), or the outbox relay.
- Choosing a delivery guarantee (at-least-once vs exactly-once) and designing the consumer to match.
- Debugging duplicate messages, out-of-order events, stuck consumers, or webhook replay storms.
- Designing Kafka topic/partition layout and consumer-group scaling.
- Implementing safe webhook retries that survive transient failures without double-processing.

## Portfolio Reality
PCMedicalist uses **background jobs + cron** (Hermes cron, PM2) for the social/security-intel pipelines, **Redis** for queues/cache, **webhooks** for Telegram/Discord/bot events and Bankr/payment callbacks, and an **outbox pattern** (see `pcmedicalist-database-deep-dive` topic 30) for write-then-publish. There is currently **no Kafka**, but the semantic principles (ordering, partitions, consumer groups) apply the moment we adopt a streaming broker, and the at-least-once + idempotency discipline applies to every queue we run today. Related: `pcmedicalist-dead-letter-queues`, `pcmedicalist-event-driven`, `pcmedicalist-idempotency`, `pcmedicalist-webhooks-hmac`.

## Topics Covered
1. At-Least-Once Delivery
2. Exactly-Once Semantics
3. Message Ordering
4. Kafka Consumer Groups
5. Partition Keys
6. Background Jobs
7. Webhook Retries

Each topic: **Semantics → Pattern → Validate → Fix**.

---

## 1. At-Least-Once Delivery
**Semantics:** The broker guarantees a message is delivered ≥1 time. Network/ack failures mean it may be delivered **more than once**. The default for nearly every real queue (SQS, Kafka, Redis Streams, Rabbit with ack).

**Pattern (make the consumer idempotent):**
```ts
// the ONLY safe way to handle at-least-once: idempotent processing
async function handleTipConfirmed(event: TipEvent) {
  const id = event.txHash;                       // natural idempotency key
  const existing = await prisma.tips.findUnique({ where: { txHash: id } });
  if (existing) return;                          // already processed — skip (no double)
  await prisma.tips.create({ data: { txHash: id, ... } });  // unique constraint backs this up
}
```
The unique constraint (`pcmedicalist-database-deep-dive` topic 13) is the hard backstop; the check is the fast path. Never assume "received once = process once".

**Validate:** Force the broker to redeliver a message 3× (nack/timeout). The consumer processes it exactly once (row count unchanged on repeats). No duplicate tips/payouts.

**Fix:** If you see duplicates, the consumer isn't idempotent — add an idempotency key + unique constraint. Don't try to make the broker "exactly once" at the cost of complexity (see topic 2); idempotency is the battle-tested answer.

---

## 2. Exactly-Once Semantics (EOS)
**Semantics:** The message is processed exactly once — no duplicates, no loss. **True EOS is nearly impossible end-to-end**; what's sold as "exactly-once" is usually *effectively-once* (idempotent producer + transactional consumer + idempotent sink).

**Pattern (effective-once via transaction + idempotency):**
```
Kafka EOS = idempotent producer (no duplicate sends) 
          + transactional consumer (read-process-write atomic per partition)
          + idempotent state write (unique key / conditional update)
=> combined effect: each message's effect applied once
```
For our scale, **prefer at-least-once + idempotent consumer (topic 1)** — it's simpler and equally correct in outcome. Only reach for Kafka transactions when you have a hard linearizability requirement and the broker supports it.

**Validate:** Under broker failure + redelivery, the final state equals "processed exactly once" — same as topic 1's test, because the idempotent sink is what actually guarantees it. No scenario produces a duplicate or a lost effect.

**Fix:** If someone demands "exactly once", explain it's effective-once via idempotency. Implement the idempotent sink; don't chase broker-level EOS unless the broker + use-case truly require it (high cost, low marginal benefit over topic 1).

---

## 3. Message Ordering
**Semantics:** Messages may arrive out of order (retries, partitions, multi-consumer). Some domains need order (per-entity event streams); most don't.

**Pattern (per-key ordering):**
```ts
// order is guaranteed PER PARTITION in Kafka; per idempotency-key in app
// to get per-user ordering: partition by userId (topic 5), OR
// accept out-of-order + reconcile via version/timestamp in the handler
async function applyEvent(e: Event) {
  const row = await getState(e.entityId);
  if (e.version <= row.version) return;          // stale/out-of-order -> skip
  await updateState(e.entityId, e.payload, e.version);
}
```
Use monotonic version/sequence per entity to drop stale events. For strict order, co-locate an entity's events on one partition (topic 5) — but that limits parallelism.

**Validate:** Send events 1,2,3 for an entity out of order (3,1,2); final state reflects version 3 (latest wins, no corruption). If strict order required, confirm same-partition co-location enforces it.

**Fix:** If out-of-order events corrupt state, add version/seq checks (drop stale). If strict order is required, partition by entity key (topic 5) and accept the parallelism trade-off. Don't rely on "they usually arrive in order".

---

## 4. Kafka Consumer Groups
**Semantics:** A consumer group = a set of consumers sharing a topic's partitions. Each partition is consumed by exactly one member. Scaling consumers past partition-count yields idle consumers.

**Pattern:**
```
topic tips.confirmed: 12 partitions
consumer-group cg-payout: 4 instances -> each gets 3 partitions
(scale to 12 instances -> 1 partition each; 13th instance = idle)
```
Rebalance on membership change redistributes partitions (brief pause). Use cooperative-sticky assignor to minimize stop-the-world rebalances. Monitor lag per partition.

**Validate:** With N consumers ≤ partitions, every partition is actively consumed (lag decreasing). Adding a consumer reduces lag (until N = partitions). A consumer crash triggers rebalance and partitions move to survivors (no data loss, brief pause).

**Fix:** If consumers are idle, you have more consumers than partitions — add partitions (note: changes key→partition mapping, affects topic 5 ordering) or reduce consumers. If a consumer is stuck, check for a poison message (topic in `pcmedicalist-dead-letter-queues`) blocking the partition. Monitor consumer lag as a first-class metric.

---

## 5. Partition Keys
**Semantics:** The key determines which partition a message lands on. Same key → same partition → preserved order + single-consumer processing. Bad key → hot partitions (topic in `pcmedicalist-distributed-patterns` topic 23) or lost ordering.

**Pattern:**
```ts
// key by entity so all its events are ordered + colocated
producer.send({ topic: 'tips.confirmed', key: tip.creatorId, value: event });
// AVOID: keying by a low-cardinality or skewed field -> hot partition
// AVOID: null key -> round-robin -> no per-entity ordering
```
Key choice = the answer to "what needs to be ordered / isolated?" Usually the entity ID (userId, creatorId, tipId). For fan-out, use a composite or a separate topic.

**Validate:** All events for one `creatorId` land on the same partition (verify via `kafka-run-class` partitioner output or a keyed hash check). Partition key distribution is even (no partition > 30% of volume). Ordering holds per key.

**Fix:** If a partition is hot, the key is skewed — re-key (salt a dominant key, or switch to a more even key). If ordering breaks, ensure the key is the entity that needs order. Never use null keys when order matters.

---

## 6. Background Jobs
**Semantics:** Work deferred from the request path (email, social posts, report generation). Must be reliable, observable, and not lose work on crash.

**Pattern (Redis/BullMQ or Hermes cron):**
```ts
// BullMQ: durable queue, retries, delayed, repeat
const queue = new Queue('social-posts', { connection: redis });
await queue.add('post', { variant: 'ig' }, {
  attempts: 3, backoff: { type: 'exponential', delay: 5000 },
  removeOnComplete: 1000, removeOnFail: 5000,
});
// worker: idempotent handler (topic 1) + correlationId (see infra-ops topic 13)
```
Rules: jobs are **idempotent** (re-run safe), have **retries with jitter** (see `pcmedicalist-resilience-engineering` topic 3), land in a **dead-letter queue** on exhaustion (see `pcmedicalist-dead-letter-queues`), and carry a correlation ID for tracing. Never put non-idempotent side effects in a retryable job.

**Validate:** Kill the worker mid-job → job re-runs on restart, completes exactly once (idempotency). A failing job exhausts retries → lands in DLQ, not lost, not retried forever. Correlation ID threads through logs.

**Fix:** If jobs are lost on crash, the queue isn't durable (use Redis/BullMQ/persistent backing, not in-memory). If retries cause duplicates, make the handler idempotent. If failures vanish, wire a DLQ + alert.

---

## 7. Webhook Retries
**Semantics:** Inbound (Telegram/Discord/Bankr → us) and outbound (us → partners) webhooks are HTTP at-least-once; retries are expected. Must be idempotent AND verify authenticity.

**Pattern (inbound — verify then dedupe):**
```ts
app.post('/webhooks/bankr', verifyHmacMiddleware, async (req, res) => {
  const evt = req.body;
  if (await seen(evt.id)) return res.status(200).end();   // idempotency: already handled
  await handle(evt);                                       // idempotent handler
  await markSeen(evt.id);
  res.status(200).end();                                   // ALWAYS 200 fast so they stop retrying
});
// outbound (us calling partner): retry with jitter, honor 429/Retry-After
async function postWebhook(url: string, body: any, attempt = 0) {
  const r = await fetch(url, { method: 'POST', body: JSON.stringify(body),
    headers: { 'Content-Type': 'application/json', 'X-Signature': sign(body) } });
  if (r.status === 429) { const wait = r.headers.get('Retry-After') || 2**attempt;
    await sleep(wait * 1000 + jitter()); return postWebhook(url, body, attempt+1); }
  if (!r.ok && attempt < 5) { await sleep((2**attempt)*1000 + jitter()); return postWebhook(url, body, attempt+1); }
  return r;
}
```
Inbound: **always return 200 quickly** after dedup (don't let your slow handler cause their retry storm). Verify HMAC (see `pcmedicalist-webhooks-hmac`). Outbound: honor `Retry-After`, use jittered backoff, cap attempts, and alert on final failure (→ DLQ).

**Validate:** Replay the same inbound webhook 5× → handled once (dedupe), 200 each time. Outbound to a flaky partner: retries with backoff, succeeds on recovery, no duplicate side effect (idempotent), alerts if all attempts fail.

**Fix:** If a partner retry-storms you, you're returning non-200 or slow — dedupe + return 200 fast. If your outbound retries cause duplicates, make the receiver idempotent or use an idempotency key. If retries hammer a down partner, add jittered backoff + `Retry-After` respect (see `pcmedicalist-resilience-engineering` topic 3, `pcmedicalist-backpressure`).

---

## Validation Checklist
- [ ] Every async consumer is idempotent (unique key + constraint) (1,2)
- [ ] Stale/out-of-order events dropped via version check (3)
- [ ] Consumer count ≤ partitions; lag monitored (4)
- [ ] Partition key = the entity needing order; distribution even (5)
- [ ] Background jobs durable + retry-with-jitter + DLQ + correlationId (6)
- [ ] Webhooks: verify HMAC, dedupe, return 200 fast; outbound honors Retry-After (7)

## Fix Recipes
- **"Duplicate tips from retries"** → #1, #2: idempotent consumer + unique constraint.
- **"Out-of-order events corrupted state"** → #3: version checks; #5: key by entity.
- **"Idle consumers / hot partition"** → #4, #5: align consumers to partitions, re-key skew.
- **"Job lost on crash"** → #6: durable queue + idempotency.
- **"Partner retry-storm"** → #7: dedupe + fast 200; outbound jitter + Retry-After.

## Cross-References
- `pcmedicalist-dead-letter-queues` — topic 6/7 failure sink
- `pcmedicalist-event-driven` — async architecture
- `pcmedicalist-idempotency` — topic 1/2/7 backstop
- `pcmedicalist-distributed-locks` — coordinating singleton processing
- `pcmedicalist-resilience-engineering` — jitter (3), backpressure (retry storms), load shedding
- `pcmedicalist-webhooks-hmac` — topic 7 inbound verification
- `pcmedicalist-database-deep-dive` — outbox (30), unique constraints (13) backing idempotency
- `pcmedicalist-cron-operations` — scheduled background work
- `pcmedicalist-distributed-patterns` — hot partitions (23), consistent hashing (7)
