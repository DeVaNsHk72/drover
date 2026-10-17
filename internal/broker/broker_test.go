package broker

import (
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisPing(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR")

	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})

	t.Cleanup(func() {
		_ = rdb.Close()
	})

	ctx := t.Context()
	err := rdb.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("Failed to ping Redis: %v", err)
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
