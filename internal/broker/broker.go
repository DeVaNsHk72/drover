package broker

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

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
