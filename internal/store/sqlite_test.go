package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

func TestSQLiteStore_CRUD(t *testing.T) {
	// Create a temp directory for the test DB
	tmpDir, err := os.MkdirTemp("", "pipeline-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	jobID := "test-job-id"

	// 1. Create Job
	job := &models.Job{
		ID:        jobID,
		Name:      "Test Job",
		Status:    models.StatusPending,
		CreatedAt: time.Now().Truncate(time.Second), // Truncate to second for SQLite comparison
		Spec: models.JobSpec{
			Name: "Test Job Spec",
			Sources: []models.SourceConfig{
				{
					ID:   "s1",
					Type: "csv",
					Path: "/path/to/file.csv",
				},
			},
		},
	}

	err = s.CreateJob(ctx, job)
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	// 2. Get Job
	fetchedJob, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if fetchedJob.Name != job.Name {
		t.Errorf("expected job name %s, got %s", job.Name, fetchedJob.Name)
	}
	if fetchedJob.Status != models.StatusPending {
		t.Errorf("expected job status %s, got %s", models.StatusPending, fetchedJob.Status)
	}

	// 3. Update Status
	err = s.UpdateJobStatus(ctx, jobID, models.StatusRunning)
	if err != nil {
		t.Fatalf("failed to update status: %v", err)
	}
	fetchedJob, err = s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if fetchedJob.Status != models.StatusRunning {
		t.Errorf("expected job status %s, got %s", models.StatusRunning, fetchedJob.Status)
	}

	// 4. Update Times
	err = s.UpdateJobTimes(ctx, jobID, true) // start time
	if err != nil {
		t.Fatalf("failed to update start time: %v", err)
	}
	fetchedJob, err = s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if fetchedJob.StartedAt == nil {
		t.Error("expected started_at to be set, got nil")
	}

	// 5. Update Metrics
	metrics := &models.JobMetrics{
		JobID:              jobID,
		RecordsIngested:    10,
		RecordsValidated:   8,
		RecordsTransformed: 7,
		RecordsAggregated:  1,
		RecordsExported:    1,
		RecordsFailed:      2,
		ProcessingRateEPS:  3.14,
		CurrentStage:       "aggregate",
	}
	err = s.UpdateJobMetrics(ctx, metrics)
	if err != nil {
		t.Fatalf("failed to update metrics: %v", err)
	}

	fetchedMetrics, err := s.GetJobMetrics(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}
	if fetchedMetrics.RecordsIngested != metrics.RecordsIngested {
		t.Errorf("expected records ingested %d, got %d", metrics.RecordsIngested, fetchedMetrics.RecordsIngested)
	}
	if fetchedMetrics.ProcessingRateEPS != metrics.ProcessingRateEPS {
		t.Errorf("expected rate %f, got %f", metrics.ProcessingRateEPS, fetchedMetrics.ProcessingRateEPS)
	}

	// 6. Log Errors
	jobErr := &models.JobError{
		JobID:        jobID,
		RecordID:     "rec-123",
		Stage:        "validate",
		ErrorMessage: "field X is missing",
		RawRecord:    `{"field":"Y"}`,
	}
	err = s.AddJobError(ctx, jobErr)
	if err != nil {
		t.Fatalf("failed to add job error: %v", err)
	}

	fetchedErrors, err := s.GetJobErrors(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get errors: %v", err)
	}
	if len(fetchedErrors) != 1 {
		t.Errorf("expected 1 error, got %d", len(fetchedErrors))
	} else {
		if fetchedErrors[0].RecordID != jobErr.RecordID {
			t.Errorf("expected record ID %s, got %s", jobErr.RecordID, fetchedErrors[0].RecordID)
		}
		if fetchedErrors[0].ErrorMessage != jobErr.ErrorMessage {
			t.Errorf("expected error msg %s, got %s", jobErr.ErrorMessage, fetchedErrors[0].ErrorMessage)
		}
	}

	// 7. Save and Get Results
	resData := map[string]interface{}{"count": float64(10), "sum": float64(45)}
	err = s.SaveJobResult(ctx, jobID, "country=USA", resData)
	if err != nil {
		t.Fatalf("failed to save job result: %v", err)
	}

	results, err := s.GetJobResults(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job results: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result row, got %d", len(results))
	} else {
		if results[0].GroupKey != "country=USA" {
			t.Errorf("expected group key country=USA, got %s", results[0].GroupKey)
		}
		if results[0].ResultData["count"].(float64) != 10 {
			t.Errorf("expected count 10, got %v", results[0].ResultData["count"])
		}
	}

	// 8. List Jobs
	jobs, err := s.ListJobs(ctx)
	if err != nil {
		t.Fatalf("failed to list jobs: %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("expected 1 job listed, got %d", len(jobs))
	}

	// 9. Delete Job
	err = s.DeleteJob(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to delete job: %v", err)
	}
	_, err = s.GetJob(ctx, jobID)
	if err == nil {
		t.Error("expected error getting deleted job, got nil")
	}
}
