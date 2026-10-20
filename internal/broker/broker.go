package broker

import (
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/enqueue.lua
var enqueueSrc string

//go:embed scripts/dequeue.lua
var dequeueSrc string

//go:embed scripts/ack.lua
var ackSrc string

var enqueueScript = redis.NewScript(enqueueSrc)
var dequeueScript = redis.NewScript(dequeueSrc)
var ackScript = redis.NewScript(ackSrc)

const defaultLease = 30 * time.Second

// ErrJobNotFound is returned when no job exists with the given ID.
var ErrJobNotFound = errors.New("job not found")

// ErrQueueEmpty is returned by Dequeue when no job is ready.
var ErrQueueEmpty = errors.New("queue empty")

// ErrNotLeased is returned by Ack when the job is not currently leased.
var ErrNotLeased = errors.New("job not leased")

// State is where a job is in its lifecycle.
type State string

const (
	StateReady     State = "ready"
	StateActive    State = "active"
	StateCompleted State = "completed"
)

// Job is one unit of work.
type Job struct {
	ID          string
	Queue       string
	Type        string
	Payload     []byte
	Priority    int
	Attempts    int
	MaxAttempts int
	State       State
	CreatedAt   time.Time
}

// Broker talks to Redis. All job state changes go through it.
type Broker struct {
	rdb *redis.Client
}

// New returns a Broker that uses the given Redis client.
func New(rdb *redis.Client) *Broker {
	return &Broker{rdb: rdb}
}

// readyKey is the ZSET of job IDs waiting to run.
func readyKey(queue string) string {
	return fmt.Sprintf("drover:{%s}:ready", queue)
}

// jobKey is the HASH holding one job's fields.
func jobKey(queue, id string) string {
	return fmt.Sprintf("drover:job:{%s}:%s", queue, id)
}

// newID returns a random, URL-safe job ID.
func newID() string {
	return rand.Text()
}

// Enqueue stores a new job and adds it to the queue's ready set.
// It returns the new job's ID.
func (b *Broker) Enqueue(ctx context.Context, queue, jobType string, payload []byte, maxAttempts int) (string, error) {
	id := newID()
	keys := []string{
		jobKey(queue, id), // job hash KEYS[1]
		readyKey(queue),   // ready queue KEYS[2]
	}
	err := enqueueScript.Run(ctx, b.rdb, keys, id, queue, jobType, payload, maxAttempts).Err()
	if err != nil {
		return "", fmt.Errorf("enqueue: %w", err)
	}
	return id, nil
}

// GetJob loads a job's hash from Redis and converts it into a Job.
func (b *Broker) GetJob(ctx context.Context, queue, id string) (*Job, error) {
	m, err := b.rdb.HGetAll(ctx, jobKey(queue, id)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(m) == 0 {
		return nil, ErrJobNotFound
	}

	attempts, err := strconv.Atoi(m["attempts"])
	if err != nil {
		return nil, fmt.Errorf("get job: bad attempts: %w", err)
	}
	maxAttempts, err := strconv.Atoi(m["max_attempts"])
	if err != nil {
		return nil, fmt.Errorf("get job: bad max_attempts: %w", err)
	}
	createdAtMs, err := strconv.ParseInt(m["created_at"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("get job: bad created_at: %w", err)
	}

	return &Job{
		ID:          m["id"],
		Queue:       m["queue"],
		Type:        m["type"],
		Payload:     []byte(m["payload"]),
		Attempts:    attempts,
		MaxAttempts: maxAttempts,
		State:       State(m["state"]),
		CreatedAt:   time.UnixMilli(createdAtMs),
	}, nil
}

func leasedKey(queue string) string {
	return fmt.Sprintf("drover:{%s}:leased", queue)
}

// Dequeue leases the oldest ready job in the queue and returns its ID.
// It returns ErrQueueEmpty if no job is waiting.
func (b *Broker) Dequeue(ctx context.Context, queue string) (string, error) {
	keys := []string{
		readyKey(queue),  // KEYS[1]
		leasedKey(queue), // KEYS[2]
	}
	id, err := dequeueScript.Run(ctx, b.rdb, keys,
		jobKey(queue, ""),           // ARGV[1]: "drover:job:{queue}:" prefix
		defaultLease.Milliseconds(), // ARGV[2]: lease in ms
	).Text()
	if errors.Is(err, redis.Nil) {
		return "", ErrQueueEmpty
	}
	if err != nil {
		return "", fmt.Errorf("dequeue: %w", err)
	}
	return id, nil
}

// Ack marks a leased job as completed.
// It returns ErrNotLeased if the job was never dequeued or was already acked.
func (b *Broker) Ack(ctx context.Context, queue, id string) error {
	keys := []string{
		leasedKey(queue),  // KEYS[1]
		jobKey(queue, id), // KEYS[2]
	}
	n, err := ackScript.Run(ctx, b.rdb, keys, id).Int()
	if err != nil {
		return fmt.Errorf("ack: %w", err)
	}
	if n == 0 {
		return ErrNotLeased
	}
	return nil
}
