package pipeline

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

type Manager struct {
	store       store.Store
	mu          sync.RWMutex
	activePipes map[string]*Pipeline
}

func NewManager(s store.Store) *Manager {
	return &Manager{
		store:       s,
		activePipes: make(map[string]*Pipeline),
	}
}

// CreateJob creates a job in the database and kicks off pipeline processing in the background
func (m *Manager) CreateJob(ctx context.Context, spec models.JobSpec) (*models.Job, error) {
	if spec.Name == "" {
		spec.Name = "pipeline-" + time.Now().Format("20060102-150405")
	}

	jobID := uuid.New().String()
	job := &models.Job{
		ID:        jobID,
		Name:      spec.Name,
		Status:    models.StatusPending,
		Spec:      spec,
		CreatedAt: time.Now().UTC(),
	}

	if err := m.store.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to create job in store: %w", err)
	}

	pipe := NewPipeline(context.Background(), jobID, spec, m.store)

	m.mu.Lock()
	m.activePipes[jobID] = pipe
	m.mu.Unlock()

	// Launch pipeline processing asynchronously
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.activePipes, jobID)
			m.mu.Unlock()
		}()
		_ = pipe.Run()
	}()

	return job, nil
}

// CancelJob triggers cancellation for an actively executing pipeline
func (m *Manager) CancelJob(ctx context.Context, jobID string) error {
	m.mu.Lock()
	pipe, exists := m.activePipes[jobID]
	if exists {
		pipe.Cancel()
		delete(m.activePipes, jobID)
	}
	m.mu.Unlock()

	job, err := m.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	if job.Status == models.StatusCompleted || job.Status == models.StatusFailed || job.Status == models.StatusCancelled {
		return fmt.Errorf("job %s is already in terminal state: %s", jobID, job.Status)
	}

	return m.store.UpdateJobStatus(ctx, jobID, models.StatusCancelled)
}

// DeleteJob cancels running pipelines, deletes metadata from DB, and removes generated export files
func (m *Manager) DeleteJob(ctx context.Context, jobID string) error {
	m.mu.Lock()
	if pipe, exists := m.activePipes[jobID]; exists {
		pipe.Cancel()
		delete(m.activePipes, jobID)
	}
	m.mu.Unlock()

	job, err := m.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	// Purge exported local files
	for _, exp := range job.Spec.Exports {
		if (exp.Type == "json" || exp.Type == "csv") && exp.Params["path"] != "" {
			_ = os.Remove(exp.Params["path"])
		}
	}

	return m.store.DeleteJob(ctx, jobID)
}

func (m *Manager) GetStore() store.Store {
	return m.store
}
