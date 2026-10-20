package broker

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisPing(t *testing.T) {
	b := newTestBroker(t)
	if err := b.rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}
}

func TestKeys(t *testing.T) {
	if readyKey("emails") != "drover:{emails}:ready" {
		t.Errorf("readyKey failed, got: %s", readyKey("emails"))
	}

	if jobKey("emails", "abc") != "drover:job:{emails}:abc" {
		t.Errorf("jobKey failed, got: %s", jobKey("emails", "abc"))
	}

	if leasedKey("emails") != "drover:{emails}:leased" {
		t.Errorf("leasedKey failed, got: %s", leasedKey("emails"))
	}
}

func TestNewID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newID()
		if seen[id] {
			t.Fatalf("newID generated a duplicate ID: %s", id)
		}
		seen[id] = true
	}
}

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")

	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	t.Cleanup(func() {
		_ = rdb.Close()
	})
	return New(rdb)
}

func TestEnqueue(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()
	queue := "test-" + newID()

	id, err := b.Enqueue(ctx, queue, "send_email", []byte("hello"), 3)
	if err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	t.Cleanup(func() {
		b.rdb.Del(context.Background(), jobKey(queue, id), readyKey(queue))
	})

	err = b.rdb.ZScore(ctx, readyKey(queue), id).Err()
	if err != nil {
		t.Fatalf("failed to find job in ready queue: %v", err)
	}

	job, err := b.GetJob(ctx, queue, id)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}

	if job.State != StateReady {
		t.Errorf("expected job state to be %s, got %s", StateReady, job.State)
	}
	if job.Type != "send_email" {
		t.Errorf("expected job type to be send_email, got %s", job.Type)
	}
	if string(job.Payload) != "hello" {
		t.Errorf("expected job payload to be hello, got %s", string(job.Payload))
	}
	if job.MaxAttempts != 3 {
		t.Errorf("expected job max attempts to be 3, got %d", job.MaxAttempts)
	}
	if job.Attempts != 0 {
		t.Errorf("expected job attempts to be 0, got %d", job.Attempts)
	}
	age := time.Since(job.CreatedAt)
	if age < 0 || age > 5*time.Second {
		t.Errorf("expected job created within 5s, got age %s", age)
	}

	_, err = b.GetJob(ctx, queue, "does-not-exist")

	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}

}

func TestDequeue(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()
	queue := "test-" + newID()

	idA, err := b.Enqueue(ctx, queue, "send_email", []byte("A"), 3)
	if err != nil {
		t.Fatalf("enqueue A: %v", err)
	}
	time.Sleep(2 * time.Millisecond) // different ms → different score → fixed order
	idB, err := b.Enqueue(ctx, queue, "send_email", []byte("B"), 3)
	if err != nil {
		t.Fatalf("enqueue B: %v", err)
	}

	t.Cleanup(func() {
		b.rdb.Del(context.Background(),
			jobKey(queue, idA), jobKey(queue, idB), readyKey(queue), leasedKey(queue))
	})

	// Oldest job comes out first.
	got, err := b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("first dequeue: %v", err)
	}
	if got != idA {
		t.Fatalf("first dequeue: want %s (A), got %s", idA, got)
	}

	job, err := b.GetJob(ctx, queue, idA)
	if err != nil {
		t.Fatalf("get job A: %v", err)
	}
	if job.State != StateActive {
		t.Errorf("want state %s, got %s", StateActive, job.State)
	}
	if job.Attempts != 1 {
		t.Errorf("want attempts 1, got %d", job.Attempts)
	}

	if err := b.rdb.ZScore(ctx, leasedKey(queue), idA).Err(); err != nil {
		t.Errorf("job A not in leased set: %v", err)
	}

	got, err = b.Dequeue(ctx, queue)
	if err != nil {
		t.Fatalf("second dequeue: %v", err)
	}
	if got != idB {
		t.Fatalf("second dequeue: want %s (B), got %s", idB, got)
	}

	_, err = b.Dequeue(ctx, queue)
	if !errors.Is(err, ErrQueueEmpty) {
		t.Errorf("third dequeue: want ErrQueueEmpty, got %v", err)
	}
}

func TestAck(t *testing.T) {
	b := newTestBroker(t)
	ctx := t.Context()
	queue := "test-" + newID()

	id, err := b.Enqueue(ctx, queue, "send_email", []byte("hello"), 3)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	t.Cleanup(func() {
		b.rdb.Del(context.Background(), jobKey(queue, id), readyKey(queue), leasedKey(queue))
	})

	// Can't ack a job nobody has taken.
	if err := b.Ack(ctx, queue, id); !errors.Is(err, ErrNotLeased) {
		t.Errorf("ack before dequeue: want ErrNotLeased, got %v", err)
	}

	if _, err := b.Dequeue(ctx, queue); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if err := b.Ack(ctx, queue, id); err != nil {
		t.Fatalf("ack: %v", err)
	}

	job, err := b.GetJob(ctx, queue, id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.State != StateCompleted {
		t.Errorf("want state %s, got %s", StateCompleted, job.State)
	}

	if err := b.rdb.ZScore(ctx, leasedKey(queue), id).Err(); !errors.Is(err, redis.Nil) {
		t.Errorf("job still in leased set after ack (err = %v)", err)
	}

	// A second ack must be rejected.
	if err := b.Ack(ctx, queue, id); !errors.Is(err, ErrNotLeased) {
		t.Errorf("second ack: want ErrNotLeased, got %v", err)
	}
}
