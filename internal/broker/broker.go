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

var enqueueScript = redis.NewScript(enqueueSrc)

type State string // new type to represent the state of the broker

const (
	StateReady     State = "ready"
	StateActive    State = "active"
	StateCompleted State = "completed"
)

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

type Broker struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Broker {
	return &Broker{rdb: rdb}
}

func readyKey(queue string) string {
	return fmt.Sprintf("drover:{%v}:ready", queue)
}

func jobKey(queue, id string) string {
	return fmt.Sprintf("drover:job:{%v}:%v", queue, id)
}

func newID() string {
	return rand.Text()
}

// Enqueue stores a new job and adds it to the queue's ready set
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

var ErrJobNotFound = errors.New("job not found")

func (b *Broker) GetJob(ctx context.Context, queue, id string) (*Job, error) {
	m, err := b.rdb.HGetAll(ctx, jobKey(queue, id)).Result()
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	if len(m) == 0 {
		return nil, ErrJobNotFound
	}
	attempts, _ := strconv.Atoi(m["attempts"])
	max_attempts, _ := strconv.Atoi(m["max_attempts"])
	created_at, _ := strconv.ParseInt(m["created_at"], 10, 64)

	return &Job{
		ID:          m["id"],
		Queue:       m["queue"],
		Type:        m["type"],
		Payload:     []byte(m["payload"]),
		Priority:    0, // Assuming priority is not stored in the hash
		Attempts:    attempts,
		MaxAttempts: max_attempts,
		State:       State(m["state"]),
		CreatedAt:   time.Unix(created_at, 0),
	}, nil
}
