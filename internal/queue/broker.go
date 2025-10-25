package queue

import (
	"context"

	"kaisan.dev/job_processor/internal/task"
)

// Broker defines the interface for a message broker that handles tasks.
// This allows swapping implementations (e.g., FIFO vs. Priority) without
// changing the worker or API server logic.
type Broker interface {
	// Enqueue adds a task to the queue.
	Enqueue(ctx context.Context, t *task.Task) error

	// Dequeue fetches a task from the queue for processing.
	// It should block until a task is available.
	Dequeue(ctx context.Context, workerID string) (*task.Task, error)

	// Close shuts down the connection to the broker.
	Close() error
}
