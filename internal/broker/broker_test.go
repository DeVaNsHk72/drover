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
