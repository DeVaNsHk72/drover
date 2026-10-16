# drover: build plan

A distributed job queue in Go on Redis. Think a small subset of Celery / BullMQ / asynq, built to be read, tested, and benchmarked.

**Goal:** a working system plus hard numbers for a resume: throughput, queue latency, and recovery time after a worker crash.

**Total time:** about 2.5 to 3 weeks at 3 to 4 hours a day.

---

## 0. Scope

### In scope
- HTTP API to submit, schedule, inspect, and cancel jobs
- Go SDK: `client.Enqueue(...)` and `worker.Handle("type", fn)`
- Worker pools with goroutines and channels
- Redis-backed ready, delayed, leased, and dead-letter sets
- Retries with exponential backoff + jitter
- Leases, heartbeats, and a reaper that recovers jobs from crashed workers
- Priorities and per-queue concurrency limits (global, across all workers)
- Backpressure on both the API side and the worker side
- Idempotency keys on enqueue
- Leader election so only one node runs maintenance loops
- Prometheus metrics, a benchmark tool, and a chaos test

### Out of scope (say so in the README)
- Exactly-once delivery (impossible in general; we do at-least-once + idempotency)
- Workflows / DAGs / durable execution (that is Temporal's job)
- Auth, multi-tenancy, a web UI beyond a tiny stats page
- Storage other than Redis

---

## 1. Architecture

```
            ┌──────────────┐        ┌──────────────────────────────┐
 clients ──▶│  API server  │──Lua──▶│            Redis             │
  (HTTP/SDK)│  (N replicas)│        │  ready / delayed / leased /  │
            └──────────────┘        │  dead ZSETs + job hashes     │
                                    └──────────────────────────────┘
                                         ▲ Lua (dequeue/ack/heartbeat)
            ┌──────────────┐             │
            │ worker node  │─────────────┘
            │  fetcher ──▶ chan ──▶ N goroutines (handlers)
            │  heartbeater │
            │  leader? ──▶ promoter + reaper
            └──────────────┘   (M worker nodes)
```

Key rule: **every state change is a single Lua script.** Redis runs scripts atomically, so no two workers can grab the same job and no job can be lost between "pop" and "mark active". This is what makes the design correct without distributed locks.

### Redis data model

All keys for a queue share the hash tag `{q}` so multi-key Lua scripts also work on Redis Cluster.

| Key | Type | Contents |
|---|---|---|
| `drover:{q}:ready` | ZSET | job IDs, score = priority band + enqueue time (lower runs first) |
| `drover:{q}:delayed` | ZSET | job IDs, score = `run_at` (unix ms) |
| `drover:{q}:leased` | ZSET | job IDs being worked on, score = lease expiry (unix ms) |
| `drover:{q}:dead` | ZSET | failed-for-good job IDs, score = time of death |
| `drover:{q}:config` | HASH | `concurrency`, `paused`, `max_depth` |
| `drover:{q}:wake` | LIST | signal list; workers `BLPOP` it instead of busy-polling |
| `drover:job:{q}:<id>` | HASH | type, payload, priority, attempts, max_attempts, timeout, state, lease_token, last_error, timestamps |
| `drover:idem:{q}:<key>` | STRING | job ID, `SET NX` with 24h TTL |
| `drover:leader` | STRING | node ID, `SET NX PX` with renewal |

Priority score: `score = (9 - priority) * 1e13 + enqueue_ms`. Priority 0 to 9, higher runs first, FIFO within a priority.

### Job lifecycle

```
 enqueue ──▶ delayed ──(promoter, run_at reached)──▶ ready
   │                                                  │
   └───────────────(no delay)─────────────────────────┘
                                                      ▼
                                          dequeue (Lua) ──▶ leased
                                                             │
                    ┌───────────── ack ──────────────────────┤──▶ completed
                    │                                        │
       fail, attempts < max ──▶ delayed (backoff)            │
       fail, attempts = max ──▶ dead                         │
       lease expired (worker died) ──(reaper)──▶ ready or dead
```

### Lua scripts (embed with `go:embed`)

| Script | Does |
|---|---|
| `enqueue.lua` | check idempotency key, check `max_depth` (backpressure), write job hash, add to ready or delayed, `LPUSH wake` |
| `dequeue.lua` | if paused or `ZCARD leased >= concurrency` return nil; else `ZPOPMIN ready`, new random `lease_token`, `attempts++`, add to leased with expiry |
| `heartbeat.lua` | if job's `lease_token` matches, push lease expiry forward; else return "lost" so the worker cancels the handler |
| `ack.lua` | token must match; remove from leased; state = completed (keep hash with TTL for inspection) |
| `fail.lua` | token must match; remove from leased; if attempts < max, add to delayed at `now + backoff`; else move to dead |
| `promote.lua` | move up to N due jobs from delayed to ready |
| `reap.lua` | for up to N leases with expiry < now: clear token, re-queue or move to dead |

The lease token is a **fencing token**: a worker whose lease expired (long GC pause, network blip) cannot ack or fail a job that was already handed to someone else.

Note: `ZCARD leased` doubles as the global concurrency counter. No separate semaphore needed.

### Leader election
- Every worker node tries `SET drover:leader <node_id> NX PX 10000` every 3s.
- The holder renews with a compare-and-`PEXPIRE` Lua script.
- Only the leader runs the promoter and reaper loops.
- Point to make in the README: promote and reap are idempotent atomic scripts, so two leaders at once (split brain) wastes work but never corrupts state. Leader election is an optimization, not a correctness requirement.

### Delivery guarantees
- **At-least-once.** A job can run twice if a worker finishes the handler but dies before `ack`.
- Enqueue-side dedupe: idempotency key.
- Handler-side dedupe: handlers receive `job.ID` and the attempt number; docs show the "check a processed-IDs set first" pattern.

### Backpressure
- API: `enqueue.lua` rejects when `ZCARD ready + ZCARD delayed >= max_depth`; API returns `429` with `Retry-After`.
- Worker: fetcher only dequeues when a goroutine slot is free (buffered channel sized to pool), so a slow handler never makes a worker hoard jobs.

### Retry backoff
`delay = min(base * 2^(attempt-1), cap) + rand(0, delay * 0.2)`. Defaults: base 1s, cap 10min, max_attempts 5. Per-job override.

---

## 2. Project layout

```
drover/
├── cmd/
│   ├── drover-server/     # HTTP API
│   ├── drover-worker/     # example worker with demo handlers (sleep, flaky, crash)
│   └── drover-bench/      # load generator + report
├── internal/
│   ├── broker/            # Redis client + Lua scripts (the core)
│   │   └── scripts/*.lua
│   ├── worker/            # pool, fetcher, heartbeater, graceful shutdown
│   ├── leader/            # election + maintenance loops
│   ├── api/               # HTTP handlers
│   └── metrics/           # Prometheus collectors
├── pkg/drover/            # public SDK: Client, Worker, Job, options
├── test/
│   ├── integration/       # real Redis via testcontainers-go
│   └── chaos/             # kill -9 workers mid-run
├── deploy/docker-compose.yml   # redis + 1 server + 3 workers + prometheus + grafana
├── bench/results/         # committed benchmark tables + charts
├── Makefile
└── plan.md
```

Libraries: `github.com/redis/go-redis/v9`, `log/slog`, `net/http` (Go 1.22+ routing, no framework), `github.com/prometheus/client_golang`, `github.com/testcontainers/testcontainers-go`.

---

## 3. HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/queues/{q}/jobs` | enqueue. Body: `type, payload, priority, delay \| run_at, max_attempts, timeout, idempotency_key`. Returns `201 {id}`, `200` on idempotent replay, `429` when full |
| `GET` | `/v1/jobs/{q}/{id}` | job state, attempts, last error |
| `DELETE` | `/v1/jobs/{q}/{id}` | cancel if still ready or delayed |
| `GET` | `/v1/queues/{q}/stats` | counts per state, oldest ready age |
| `PUT` | `/v1/queues/{q}/config` | `concurrency`, `paused`, `max_depth` |
| `GET` | `/v1/queues/{q}/dead` | list dead jobs (paged) |
| `POST` | `/v1/queues/{q}/dead/{id}/retry` | move a dead job back to ready |
| `GET` | `/healthz`, `/metrics` | health, Prometheus |

---

## 4. Milestones and phases

Each phase ends with an acceptance test. Do not start the next phase until it passes.

### Milestone A: it works (about 4 days)

**Phase 0. Setup (1 hour)**
- `brew install go` (Go is not installed yet), `go mod init github.com/DeVaNsHk72/drover`
- `deploy/docker-compose.yml` with Redis 7+
- Makefile targets: `run-redis`, `test`, `lint`, `bench`
- GitHub Actions: `go vet`, `golangci-lint`, `go test -race` with a Redis service container
- Done when: `make test` is green on an empty test in CI.

**Phase 1. Enqueue, dequeue, ack (1.5 days)**
- `broker`: `Enqueue`, `Dequeue`, `Ack` as Lua scripts. No leases yet, plain ready → active → completed.
- `pkg/drover`: `Client.Enqueue(ctx, queue, type, payload, opts...)`
- One worker loop, one goroutine.
- Accept: enqueue 1,000 jobs, worker processes all 1,000, every job hash says `completed`, ready set empty.

**Phase 2. Worker pool + graceful shutdown (1 day)**
- Fetcher goroutine → buffered channel → N handler goroutines.
- Per-job `context.WithTimeout` from the job's `timeout`.
- `SIGTERM`: stop fetching, wait for in-flight jobs up to a grace period, then exit.
- `drover-worker` binary with demo handlers.
- Accept: 10,000 jobs with 16 goroutines finish about 16x faster than with 1 (sleep handler). `go test -race` clean. `SIGTERM` mid-run loses zero jobs.

**Phase 3. HTTP API (1 day)**
- Routes from section 3 except dead-letter and config.
- Request validation, JSON errors, `slog` request logging.
- Accept: `curl` enqueue → `GET` shows state moving `ready → active → completed`.

### Milestone B: it is reliable (about 6 days)

**Phase 4. Delayed jobs, retries, DLQ (1.5 days)**
- `delayed` ZSET, `promote.lua`, `fail.lua`, backoff formula.
- Dead-letter list + retry endpoint.
- Accept: a job delayed 5s starts between 5s and 6s. A handler that always errors runs exactly `max_attempts` times with growing gaps, then lands in `dead`. Retrying it from `dead` runs it again.

**Phase 5. Leases, heartbeats, crash recovery (2 days) — the core of the project**
- `dequeue.lua` issues a lease token and expiry (default 30s).
- Heartbeater goroutine extends leases every `lease/3` for all in-flight jobs.
- `heartbeat.lua` returns "lost" → worker cancels that handler's context.
- `reap.lua` re-queues expired leases.
- `ack`/`fail` reject stale tokens.
- Accept (integration test): start worker, enqueue 100 slow jobs, `SIGKILL` the worker process, start a new one. All 100 complete. Measure time from kill to re-pickup. A stale-token ack returns an error and changes nothing.

**Phase 6. Priorities, concurrency limits, backpressure, idempotency (1.5 days)**
- Priority score in `enqueue.lua`.
- `config` hash; `dequeue.lua` enforces `concurrency` and `paused`.
- `max_depth` → `429`.
- Idempotency key `SET NX`.
- Accept: with a backlog, all priority 9 jobs start before any priority 0. With `concurrency=3` and 3 worker nodes of 10 goroutines each, never more than 3 jobs leased at once (sample `ZCARD leased` every 10ms). Same idempotency key twice returns the same ID and one job.

**Phase 7. Leader election + horizontal scaling (1 day)**
- `internal/leader`: acquire, renew, step down on failure.
- Promoter and reaper only run on the leader.
- Accept: run 3 workers, kill the leader, a new leader takes over within about 10s and delayed jobs keep being promoted. Force two leaders in a test: no job duplicated or lost.

### Milestone C: it proves it (about 4 days)

**Phase 8. Observability (1 day)**
- Prometheus: `jobs_enqueued_total`, `jobs_completed_total{status}`, `queue_depth{state}`, `job_wait_seconds` (enqueue → start histogram), `job_run_seconds`, `leases_reaped_total`, `leader{node}`.
- Grafana dashboard JSON in `deploy/`.
- Accept: `docker compose up`, run the bench, dashboard shows live throughput and depth.

**Phase 9. Benchmarks + chaos (2 days)**
- `drover-bench`: enqueue rate, worker count, handler duration, job count as flags. Outputs a markdown table.
- Experiments (record hardware: MacBook model, Redis version):
  1. Throughput (jobs/s) with no-op handler vs workers: 1, 2, 4, 8 nodes x 32 goroutines
  2. Queue latency p50 / p95 / p99 (enqueue → start) at 50% and 90% of max throughput
  3. Recovery time after `SIGKILL` of 1 of 4 workers, lease TTL 5s / 15s / 30s
  4. Duplicate execution rate under repeated crashes (expect small and nonzero; that is honest at-least-once)
  5. Optional: compare with asynq or BullMQ on the same box
- Chaos script: loop that `kill -9`s a random worker every 20s for 5 minutes; assert every job completed at least once.
- Accept: `bench/results/README.md` with tables and one chart per experiment.

**Phase 10. Polish (1 day)**
- README: architecture diagram, guarantees, design trade-offs, results table, quickstart (`docker compose up` in under 2 minutes).
- `examples/`: email-sender style worker, scheduled job.
- Optional: short blog post "Building at-least-once delivery on Redis with Lua".

---

## 5. Tests that matter

| Test | Proves |
|---|---|
| Two goroutines dequeue the same queue 10k times | no job handed out twice while leased |
| Ack with stale lease token | fencing works |
| `SIGKILL` mid-job | recovery, measured |
| Two leaders forced at once | maintenance is idempotent |
| `concurrency=N` sampled under load | global limit holds across nodes |
| `go test -race ./...` | no data races in pool / heartbeater |

---

## 6. Resume bullets (fill the numbers from Phase 9)

- Built **drover**, a distributed job queue in Go and Redis with at-least-once delivery, leases with fencing tokens, retries with exponential backoff, dead-letter queues, priorities, and global per-queue concurrency limits.
- Sustained **__k jobs/s** across 8 worker nodes with **p99 queue latency of __ ms**; all state transitions implemented as atomic Redis Lua scripts.
- Recovered jobs from crashed workers in **__ s** (lease TTL 15s) with **0 lost jobs** across a 5-minute chaos test killing a worker every 20s.

Interview talking points: why at-least-once and not exactly-once, why Lua instead of `MULTI`/`WATCH`, fencing tokens, why leader election is not needed for correctness here, backpressure at both ends.

---

## 7. Risks

| Risk | Mitigation |
|---|---|
| Redis is a single point of failure | out of scope; note Redis Sentinel / AOF `everysec` in README, and that AOF loss window can lose acked enqueues |
| Clock skew between workers | all lease and delay times use Redis `TIME` inside Lua, never the worker clock |
| Busy polling burns CPU | `BLPOP` on the `wake` list with a 1s timeout, then fall back to dequeue |
| Large payloads slow Redis | cap payload at 512 KB in the API |
| Scope creep into Temporal | workflows are a non-goal; stop at Phase 10 |

---

## 8. Today

1. `brew install go`
2. `cd ~/Documents/drover && go mod init github.com/DeVaNsHk72/drover`
3. Write `deploy/docker-compose.yml` with one Redis service and run `docker compose up -d`
