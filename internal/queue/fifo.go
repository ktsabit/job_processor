package queue

import (
	"context"
	"encoding/json"

	"kaisan.dev/job_processor/internal/task"

	"github.com/redis/go-redis/v9"
)

// FIFOQueue implements the Broker interface using a simple Redis LIST (LPUSH/BRPOP).
type FIFOQueue struct {
	client    *redis.Client
	queueName string
}

// NewFIFOQueue creates a new FIFO queue handler.
func NewFIFOQueue(client *redis.Client, queueName string) *FIFOQueue {
	return &FIFOQueue{client: client, queueName: queueName}
}

// Enqueue adds a task to the end of the list.
func (q *FIFOQueue) Enqueue(ctx context.Context, t *task.Task) error {
	taskBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return q.client.LPush(ctx, q.queueName, taskBytes).Err()
}

// Dequeue retrieves a task from the start of the list, blocking until one is available.
func (q *FIFOQueue) Dequeue(ctx context.Context, workerID string) (*task.Task, error) {
	// BRPOP is a blocking operation. Timeout is 0, so it blocks indefinitely.
	res, err := q.client.BRPop(ctx, 0, q.queueName).Result()
	if err != nil {
		return nil, err
	}

	// res is a slice where res[0] is the queue name and res[1] is the value.
	var t task.Task
	if err := json.Unmarshal([]byte(res[1]), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (q *FIFOQueue) Close() error {
	return q.client.Close()
}
