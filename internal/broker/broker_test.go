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
