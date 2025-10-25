package worker

import (
	"context"
	"encoding/json"

	// "fmt"
	"kaisan.dev/job_processor/internal/queue"
	// "kaisan.dev/job_processor/internal/task"
	"log"
	"sync"
)

// TaskFunc is the function signature for a task handler.
type TaskFunc func(ctx context.Context, payload json.RawMessage) error

// Worker processes tasks from a queue.
type Worker struct {
	id          string
	broker      queue.Broker
	taskMap     map[string]TaskFunc // Maps task type to its handler function
	concurrency int
}

// New creates a new Worker.
func New(id string, broker queue.Broker, concurrency int) *Worker {
	return &Worker{
		id:          id,
		broker:      broker,
		taskMap:     make(map[string]TaskFunc),
		concurrency: concurrency,
	}
}

// RegisterTask associates a task type with a handler function.
func (w *Worker) RegisterTask(taskType string, handler TaskFunc) {
	w.taskMap[taskType] = handler
}

// Start begins the worker's processing loop.
func (w *Worker) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			log.Printf("Worker routine #%d starting", workerNum)
			for {
				select {
				case <-ctx.Done():
					log.Printf("Worker routine #%d shutting down", workerNum)
					return
				default:
					w.processOne(ctx, workerNum)
				}
			}
		}(i)
	}
	wg.Wait()
}

// processOne dequeues and processes a single task.
func (w *Worker) processOne(ctx context.Context, workerNum int) {
	log.Printf("[Worker #%d] Waiting for task...", workerNum)
	t, err := w.broker.Dequeue(ctx, w.id)
	if err != nil {
		log.Printf("Error dequeuing task: %v", err)
		return // Or sleep for a bit before retrying
	}

	log.Printf(
		"[Worker #%d] Processing task %s (Type: %s, Priority: %d)",
		workerNum, t.ID, t.Type, t.Priority,
	)

	handler, ok := w.taskMap[t.Type]
	if !ok {
		log.Printf("Error: No handler registered for task type '%s'", t.Type)
		// Here you would normally mark the task as failed.
		return
	}

	// Execute the handler
	if err := handler(ctx, t.Payload); err != nil {
		log.Printf("Error processing task %s: %v", t.ID, err)
		// Mark task as failed.
	} else {
		log.Printf("Successfully completed task %s", t.ID)
		// Mark task as completed.
	}
	// TODO: Save results and metrics to PostgreSQL.
}
