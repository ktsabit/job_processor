package task

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// PriorityLevel defines the priority of a task.
type PriorityLevel int

const (
	Low    PriorityLevel = 1
	Medium PriorityLevel = 5
	High   PriorityLevel = 10
	// Add more levels as needed
)

// Status represents the current state of a task.
type Status string

const (
	StatusPending   Status = "pending"
	StatusActive    Status = "active"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Task represents the core unit of work.
// It's designed to be serializable to JSON for storage in Redis.
type Task struct {
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"` // e.g., "send_email", "generate_report"
	Payload   json.RawMessage `json:"payload"`
	Priority  PriorityLevel   `json:"priority"`
	CreatedAt time.Time       `json:"created_at"` // Crucial for the aging mechanism
	Status    Status          `json:"status"`
}

// NewTask creates a new task with a given type, priority, and payload.
func NewTask(taskType string, priority PriorityLevel, payload json.RawMessage) *Task {
	return &Task{
		ID:        uuid.New(),
		Type:      taskType,
		Payload:   payload,
		Priority:  priority,
		CreatedAt: time.Now().UTC(),
		Status:    StatusPending,
	}
}
