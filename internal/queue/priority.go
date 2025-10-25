package queue

import (
	"context"
	"encoding/json"
	"time"

	"kaisan.dev/job_processor/internal/task"

	"github.com/redis/go-redis/v9"
)

// PriorityQueue implements the Broker interface using a Redis SORTED SET.
// The score of each member will represent its effective priority.
type PriorityQueue struct {
	client      *redis.Client
	queueName   string
	agingFactor float64 // How much priority increases per second of waiting.
}

// NewPriorityQueue creates a new priority queue handler.
func NewPriorityQueue(client *redis.Client, queueName string, agingFactor float64) *PriorityQueue {
	return &PriorityQueue{
		client:      client,
		queueName:   queueName,
		agingFactor: agingFactor,
	}
}

// calculateScore computes the effective priority.
// Higher score = higher priority.
func (q *PriorityQueue) calculateScore(t *task.Task) float64 {
	// Effective Priority = Base Priority + (Time Waited * Aging Factor)
	timeWaited := time.Since(t.CreatedAt).Seconds()
	agingBonus := timeWaited * q.agingFactor
	return float64(t.Priority) + agingBonus
}

// Enqueue adds a task to the sorted set with its initial priority score.
func (q *PriorityQueue) Enqueue(ctx context.Context, t *task.Task) error {
	taskBytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	// Note: For a true aging implementation, a background process would need to
	// periodically update the scores of waiting tasks. For this foundational
	// implementation, we will use a simplified dequeue logic. The score is calculated
	// only at enqueue time. A more robust implementation is a key part of your research.
	member := &redis.Z{
		Score:  float64(t.Priority), // Initial score is just the base priority.
		Member: taskBytes,
	}
	return q.client.ZAdd(ctx, q.queueName, *member).Err()
}

// Dequeue retrieves the task with the highest priority.
// This is a simplified version. A real-world scenario might use a Lua script for atomicity.
func (q *PriorityQueue) Dequeue(ctx context.Context, workerID string) (*task.Task, error) {
	// BZPOPMIN/MAX is ideal for this, available in Redis 5.0.0+
	// It blocks until a member is available and atomically pops the one with the highest score.
	res, err := q.client.BZPopMax(ctx, 0, q.queueName).Result()
	if err != nil {
		return nil, err
	}

	// res.Member is the task data (JSON string)
	var t task.Task
	if err := json.Unmarshal([]byte(res.Member.(string)), &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (q *PriorityQueue) Close() error {
	return q.client.Close()
}
