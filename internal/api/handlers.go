package api

import (
	"encoding/json"
	"log"
	"net/http"

	"kaisan.dev/job_processor/internal/queue"
	"kaisan.dev/job_processor/internal/task"
)

// APIHandler holds dependencies for the API, like the message broker.
// Using a struct for handlers is a standard Go practice for dependency injection.
type APIHandler struct {
	broker queue.Broker
}

// NewHandler creates a new APIHandler with the required dependencies.
func NewHandler(b queue.Broker) *APIHandler {
	return &APIHandler{broker: b}
}

// CreateTaskRequest defines the expected structure of a new task request body.
type CreateTaskRequest struct {
	Type     string             `json:"type"`
	Priority task.PriorityLevel `json:"priority"`
	Payload  json.RawMessage    `json:"payload"`
}

// CreateTaskResponse defines the structure of the response after a task is created.
type CreateTaskResponse struct {
	Message string `json:"message"`
	TaskID  string `json:"task_id"`
}

// CreateTask is the HTTP handler for creating a new task.
// It parses the request, creates a task, and enqueues it.
func (h *APIHandler) CreateTask(w http.ResponseWriter, r *http.Request) {
	// 1. Decode the incoming JSON request.
	var req CreateTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// 2. Validate the request (simple validation for now).
	if req.Type == "" {
		http.Error(w, "Task 'type' is required", http.StatusBadRequest)
		return
	}

	// 3. Create a new task object.
	newTask := task.NewTask(req.Type, req.Priority, req.Payload)

	// 4. Enqueue the task using the broker.
	if err := h.broker.Enqueue(r.Context(), newTask); err != nil {
		log.Printf("ERROR: Failed to enqueue task: %v", err)
		http.Error(w, "Could not process task", http.StatusInternalServerError)
		return
	}

	log.Printf("SUCCESS: Enqueued task %s of type '%s'", newTask.ID.String(), newTask.Type)

	// 5. Send a success response back to the client.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(CreateTaskResponse{
		Message: "Task successfully enqueued",
		TaskID:  newTask.ID.String(),
	})
}
