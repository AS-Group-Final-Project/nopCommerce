# Feasibility Spike — Transactional Outbox under Broker Outage

**Purpose:** De-risk the foundational reliability claim of the design with one small, falsifiable experiment, before any further iteration depends on it.

---

## 1. Why this is the riskiest part of the design

Five iterations of the target architecture rest on one assumption: that an order placed in nopCommerce will eventually reach OpenBoxes **exactly once**, even if RabbitMQ is unreachable at the moment of order commit, and even if the bridge consumer is offline for an extended period.

That assumption is the joint claim of four decisions:

| ADR | Decision | What it contributes to the claim |
| --- | --- | --- |
| ADR-001 | RabbitMQ as broker | The transport itself |
| ADR-003 | Durable queues + persistent delivery + manual ack | Messages survive broker restart |
| ADR-004 | Transactional outbox | No publish happens outside a DB transaction; no broker-down window during commit |
| ADR-005 | Dispatcher via `IScheduleTask` | The outbox is drained off the checkout thread |

Every other iteration — allocation gate (Iter 3), carrier integration (Iter 4), OpenBoxes polling (Iter 5) — assumes this layer works. If the dispatcher silently drops rows on broker failure, or the broker drops messages on restart, or the consumer creates duplicates on redelivery, **the entire reliability story of QAS-1 and QAS-4 collapses.**

That makes this loop the riskiest part of the design:

- **Highest blast radius if wrong** — invalidates four ADRs and two QAS at once
- **Most novel mechanism** — combines three behaviours (outbox commit, dispatcher polling, durable redelivery) that interact non-trivially
- **Hardest to argue without evidence** — "messages are not lost" is a claim that only an outage-and-recovery experiment can defend

The OpenBoxes API contract spike documented in `08-risk-and-validation-plan.md` §3 is a *higher-priority operational* spike (a wrong assumption forces rework), but it is not an *architectural* spike — it tests one HTTP endpoint, not a multi-component reliability mechanism. This document covers the architectural one.

---

## 2. Hypothesis

> If RabbitMQ is unavailable for an extended window during which orders are committed, and then the broker is restored, **every committed order will appear exactly once in OpenBoxes within 60 seconds of broker recovery, with no operator intervention.**

This hypothesis combines QAS-1 (Reliability) and QAS-4 (Recoverability) into a single falsifiable claim. It can be invalidated by any of three failure modes:

1. **Loss** — a committed order does not reach OpenBoxes at all
2. **Duplication** — a committed order produces more than one fulfillment row
3. **Recovery latency** — orders eventually arrive, but later than 60 s after broker restart

If any of the three is observed, ADR-004 / ADR-009 must be revisited before further iterations are defended.

---

## 3. Setup

The spike is **designed to run on a single developer laptop** once the foundational components specified in Iterations 1–3 are realised. The components below are the spike's runtime prerequisites; some already exist in the repository baseline, others are introduced by the iterations they are tied to.

| Component | Status | Role in the spike |
| --- | --- | --- |
| `nopCommerce` (web) | Already in repository (baseline) | Order placement + outbox dispatcher (`OutboxDispatcherTask`, 1 s) |
| `MSSQL` | Already in `docker-compose.yml` | nopCommerce DB; holds `OutboxMessage` table |
| `RabbitMQ` | To add to compose (Iter 1) | Broker — **deliberately stopped and restarted during the run** |
| `Nop.Plugin.Messaging.RabbitMq` (with outbox writer + dispatcher) | To implement (Iter 1–2) | Publishes outbox rows to RabbitMQ on a 1 s tick |
| `VerdeMart.OpenBoxesBridge` | To implement (Iter 3) | Idempotent consumer; writes to its own `processed_orders` dedup store |
| `OpenBoxes` (or stub) | To add to compose (Iter 3) | Receives fulfillment-creation calls; the spike counts rows here as ground truth |

A single test product is configured with sufficient stock so the allocation gate (ADR-006) is not exercised — this spike isolates the publish-deliver-consume path, not the consistency path.

**Estimated execution time:** approximately 15 minutes per pass (≈1 min baseline, 5 min outage with order placement, ≈1 min recovery, plus reset and observation), giving an upper bound of around 30 minutes for the two passes described in §4.

---

## 4. Method

```
T = 0 s       Place 5 orders for the test product (web checkout).
              Verify: 5 rows in OutboxMessage with Status = 'Sent';
                      5 rows visible in OpenBoxes.
                      Baseline OK.

T = 60 s      docker compose stop rabbitmq

T = 60–360 s  Place 12 orders during the broker outage.
              Verify periodically:
                - All 12 orders are committed in nopCommerce (OrderId returned to the user)
                - All 12 rows present in OutboxMessage with Status = 'Pending'
                - The OutboxDispatcherTask logs broker-unreachable warnings but does NOT
                  mark rows as failed
                - Bridge consumer is idle (queue is empty because broker is down)
                - OpenBoxes still shows only the 5 baseline rows

T = 360 s     docker compose start rabbitmq

T = 360+ s    Start a stopwatch.
              Watch:
                - OutboxDispatcherTask drains the 12 Pending rows
                - Bridge consumer processes 12 deliveries
                - 12 new rows appear in OpenBoxes (ground truth)
              Stop the stopwatch when the 12th OpenBoxes row is created.
              Record:
                - Total elapsed time from broker start to last OpenBoxes row
                - Per-order latency distribution
                - Final count in OpenBoxes (must equal 5 + 12 = 17)
                - Final count in bridge processed_orders dedup store (must equal 12)
```

A second pass repeats the run, but stops the **bridge** instead of the broker, to isolate consumer-outage recovery (the QAS-4 half of the hypothesis).

---

## 5. Success criteria

The spike succeeds **only if all four conditions hold** on both passes:

| # | Criterion | Threshold |
| - | --- | --- |
| 1 | Every committed order produces exactly one OpenBoxes fulfillment row | 17 / 17 (5 baseline + 12 placed during outage) |
| 2 | Zero rows in any DLQ | 0 |
| 3 | Time from broker (or bridge) recovery to last fulfillment row created | ≤ 60 s |
| 4 | Bridge `processed_orders` dedup store contains exactly one row per `OrderGuid` | 12 / 12 |

Any other outcome — partial loss, duplication, or recovery latency above 60 s — falsifies the hypothesis and triggers a follow-up ADR.

---

## 6. What we will measure (instrumentation)

- **Outbox dispatcher logs** — broker-unreachable retry behaviour, dispatch latency per row
- **RabbitMQ Management Console** — queue depth before/during/after outage; redelivery counters
- **Bridge logs** — first-delivery vs. redelivery, dedup hits
- **OpenBoxes row count** — sampled every 5 seconds during recovery window
- **Wall-clock stopwatch** — broker restart → last OpenBoxes row created

Numerical results will be appended to this document under §8 once the spike is run.

---

## 7. What this spike does NOT cover

The spike is intentionally narrow. It does not:

- **Validate concurrent allocation** (QAS-2). That is a separate spike (concurrent web + POS on the last unit) listed in `08-risk-and-validation-plan.md` §4.
- **Validate OpenBoxes contract behaviour** (duplicate-detection response shape, fulfillment endpoint semantics). That is the higher-priority *operational* spike, and is the next one to run.
- **Validate carrier webhook latency** (QAS-5). Iteration 4 has its own latency measurement.
- **Stress-test broker throughput.** The volumes used (5 + 12 orders) are demo-scale; the goal is correctness under outage, not throughput.
- **Validate horizontal-scale dispatcher behaviour.** Single-node deployment is explicit in ADR-005.

---

## 8. Results

*To be filled in once the spike is run.*

| Pass | Outage type | Duration | Orders placed during outage | OpenBoxes rows after recovery | DLQ rows | Recovery latency | Hypothesis confirmed? |
| ---- | ----------- | -------- | ---------------------------- | ------------------------------ | --------- | ----------------- | --------------------- |
| 1 | RabbitMQ stopped | 5 min | 12 | — | — | — s | — |
| 2 | Bridge stopped | 5 min | 12 | — | — | — s | — |

Observations and any anomalies are recorded inline below the table.

---

## 9. Impact on the architecture if the spike fails

| Failure mode | Triggered revisit | Likely follow-up |
| --- | --- | --- |
| Loss of a committed order | ADR-004, ADR-005 | Move dispatcher to a separate process with at-least-once delivery guarantees independent of nopCommerce process lifetime |
| Duplicate rows in OpenBoxes | ADR-009 | Strengthen the bridge dedup key (e.g. composite of `OrderGuid` + `Version`) or add OpenBoxes-side idempotency token |
| Recovery latency > 60 s | ADR-005 | Tune dispatcher poll interval, or extract dispatcher to its own host; possibly introduce broker-side `x-message-ttl` policies to bound queue lag |

If the spike succeeds on both passes, the QAS-1 and QAS-4 response measures stated in `04-qas.md` are defensible by evidence rather than by argument alone, and the foundational reliability layer is locked in for the remaining iterations.
