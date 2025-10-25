package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kaisan.dev/job_processor/internal/api"
	"kaisan.dev/job_processor/internal/queue"

	"github.com/redis/go-redis/v9"
)

func main() {
	// ---- Configuration ----
	// In a real app, load this from config files or env vars.
	redisAddr := "localhost:6379"
	httpAddr := ":8080"
	usePriorityQueue := true // <<-- SET THIS TO 'false' TO USE FIFO
	queueName := "tasks"

	// ---- Redis Connection ----
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		log.Fatalf("FATAL: Could not connect to Redis: %v", err)
	}

	// ---- Broker Initialization ----
	var broker queue.Broker
	if usePriorityQueue {
		log.Println("INFO: API server configured to use Priority Queue")
		// The aging factor is not used by the producer, only the consumer,
		// but we still need to provide it for the constructor.
		broker = queue.NewPriorityQueue(rdb, queueName, 0.1)
	} else {
		log.Println("INFO: API server configured to use FIFO Queue")
		broker = queue.NewFIFOQueue(rdb, queueName)
	}

	// ---- HTTP Server Setup ----
	handler := api.NewHandler(broker)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", handler.CreateTask) // Register the handler

	server := &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	// ---- Graceful Shutdown Handling ----
	go func() {
		log.Printf("INFO: API server starting on %s", httpAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("FATAL: Could not start server: %v", err)
		}
	}()

	// Wait for termination signal
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan

	log.Println("INFO: Shutting down server...")

	// Create a context with a timeout for shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("FATAL: Server shutdown failed: %v", err)
	}

	log.Println("INFO: Server gracefully stopped.")
}
