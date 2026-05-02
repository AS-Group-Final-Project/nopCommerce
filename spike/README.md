# Feasibility Spike — Transactional Outbox under Broker Outage

Proves the joint claim of ADR-003, ADR-004, ADR-005 and ADR-009 before any
production iteration depends on it. See
[`docs/architecture/11-feasibility-spike.md`](../docs/architecture/11-feasibility-spike.md)
for the full hypothesis, method, and results table.

---

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download)
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose

---

## How to run (3 steps)

### Step 1 — Start RabbitMQ

```bash
cd spike
docker compose up -d
```

Management console available at http://localhost:15672 (guest / guest).
Wait ~10 seconds for the broker to be ready before proceeding.

### Step 2 — Run the spike

Open a **new terminal** in the `spike/OutboxSpike` folder:

```bash
cd spike/OutboxSpike
dotnet run
```

You will see three roles printing interleaved output:

```
[checkout]   Order #1 committed  guid=3f7a1b2c…  → outbox Pending
[dispatcher] Published  guid=3f7a1b2c…  → Sent
[bridge]     OpenBoxes row created  order=#1  guid=3f7a1b2c…  total=1
```

Every 10 seconds a status line prints:

```
[status]  outbox Pending=0 Sent=3 | OpenBoxes rows=3 | DLQ depth=0
```

### Step 3 — Exercise the outage (the actual spike)

While the program is running, in a **third terminal**:

```bash
# Stop the broker — simulates RabbitMQ outage
cd spike
docker compose stop rabbitmq
```

Watch the dispatcher log:
```
[dispatcher] Broker down — 1 row(s) remain Pending
[dispatcher] Broker down — 2 row(s) remain Pending
...
```
Orders keep being committed (checkout keeps running). No rows are lost.

After ~1 minute (to accumulate several Pending rows), restart:

```bash
docker compose start rabbitmq
```

Watch the dispatcher drain all Pending rows and the bridge consume them:
```
[dispatcher] Published  guid=…  → Sent
[dispatcher] Published  guid=…  → Sent
[bridge]     OpenBoxes row created  order=#4  guid=…  total=4
[bridge]     OpenBoxes row created  order=#5  guid=…  total=5
```

Press **Ctrl+C** to stop. The final status line shows the ground-truth counts.

---

## What to record (fills §8 of the spike document)

| What | Where to look |
|---|---|
| Orders placed during outage | `[checkout]` lines while broker was stopped |
| OpenBoxes rows after recovery | Final `[status]` or `[bridge] total=N` line |
| DLQ depth | Final `[status]` line — must be 0 |
| Recovery latency | Stopwatch from `docker compose start` to last `[bridge]` line |

---

## Clean up

```bash
# Remove the SQLite DB created by the spike
rm spike/OutboxSpike/outbox_spike.db

# Stop and remove the RabbitMQ container
cd spike
docker compose down -v
```

---

## What this spike does NOT test

- Concurrent allocation / oversell (QAS-2) — separate spike
- OpenBoxes HTTP API contract — separate spike
- Carrier webhook latency (QAS-5) — Iteration 4
- Horizontal-scale dispatcher — explicitly deferred by ADR-005
