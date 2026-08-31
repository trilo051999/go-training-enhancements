package store

import (
	"context"
	"github.com/trilochanparida/go-pipeline/internal/models"
)

type ResultRow struct {
	GroupKey   string                 `json:"group_key"`
	ResultData map[string]interface{} `json:"result_data"`
}

type Store interface {
	// Job Management
	CreateJob(ctx context.Context, job *models.Job) error
	GetJob(ctx context.Context, id string) (*models.Job, error)
	ListJobs(ctx context.Context) ([]*models.Job, error)
	UpdateJobStatus(ctx context.Context, id string, status models.JobStatus) error
	UpdateJobTimes(ctx context.Context, id string, start bool) error // Sets started_at or ended_at
	DeleteJob(ctx context.Context, id string) error

	// Metrics & Progress
	GetJobMetrics(ctx context.Context, jobID string) (*models.JobMetrics, error)
	UpdateJobMetrics(ctx context.Context, metrics *models.JobMetrics) error

	// Error Logs
	AddJobError(ctx context.Context, jobErr *models.JobError) error
	GetJobErrors(ctx context.Context, jobID string) ([]*models.JobError, error)

	// Aggregated Results Storage
	SaveJobResult(ctx context.Context, jobID string, groupKey string, resultData map[string]interface{}) error
	GetJobResults(ctx context.Context, jobID string) ([]ResultRow, error)
}
