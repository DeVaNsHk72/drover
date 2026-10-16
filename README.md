# drover

A lightweight distributed job queue in Go, backed by Redis. A small, readable alternative to Celery / BullMQ.

- At-least-once delivery with job leases and heartbeats
- Delayed jobs, retries with exponential backoff, dead-letter queues
- Priorities, per-queue concurrency limits, backpressure
- Crash recovery: jobs held by a dead worker are re-queued automatically
- Horizontal scaling with leader election for background maintenance

Status: planning. See [plan.md](plan.md).
