package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kaisan.dev/job_processor/internal/queue"
	"kaisan.dev/job_processor/internal/worker"

	"github.com/redis/go-redis/v9"
)

// A simple example task handler.
func handleEmailDelivery(ctx context.Context, payload json.RawMessage) error {
	var p struct {
		Recipient string `json:"recipient"`
		Body      string `json:"body"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}

	log.Printf("Sending email to %s", p.Recipient)
	time.Sleep(1 * time.Second) // Simulate work
	return nil
}

func main() {
	// ---- Configuration ----
	// In a real app, load this from config files or env vars.
	redisAddr := "localhost:6379"
	usePriorityQueue := true // <<-- SET THIS TO 'false' TO TEST FIFO
	queueName := "tasks"
	concurrency := 5

	// ---- Redis Connection ----
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("Could not connect to Redis: %v", err)
	}

	// ---- Broker Initialization ----
	var broker queue.Broker
	if usePriorityQueue {
		log.Println("Using Priority Queue")
		// The aging factor is a key variable for your experiment.
		agingFactor := 0.1 // Priority increases by 0.1 every second.
		broker = queue.NewPriorityQueue(rdb, queueName, agingFactor)
	} else {
		log.Println("Using FIFO Queue")
		broker = queue.NewFIFOQueue(rdb, queueName)
	}

	// ---- Worker Setup ----
	w := worker.New("worker-1", broker, concurrency)
	w.RegisterTask("send_email", handleEmailDelivery)
	// Register more task handlers here.

	// ---- Graceful Shutdown Handling ----
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		w.Start(ctx)
	}()

	// Wait for termination signal
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan
	log.Println("Shutting down worker...")
	cancel()                    // Signal the worker to stop
	time.Sleep(2 * time.Second) // Give it time to finish current tasks
	log.Println("Worker stopped.")
}
