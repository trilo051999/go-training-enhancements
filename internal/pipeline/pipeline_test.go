package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

func TestValidation(t *testing.T) {
	rules := []models.ValidationRule{
		{Field: "age", Rule: "min", Value: 18.0},
		{Field: "age", Rule: "max", Value: 65.0},
		{Field: "name", Rule: "required"},
		{Field: "email", Rule: "regex", Value: `^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$`},
		{Field: "score", Rule: "type", Value: "float"},
	}

	tests := []struct {
		name    string
		payload map[string]interface{}
		wantErr bool
	}{
		{
			name: "valid record",
			payload: map[string]interface{}{
				"age":   30.0,
				"name":  "Alice",
				"email": "alice@gmail.com",
				"score": 95.5,
			},
			wantErr: false,
		},
		{
			name: "missing required name",
			payload: map[string]interface{}{
				"age":   30.0,
				"email": "alice@gmail.com",
				"score": 95.5,
			},
			wantErr: true,
		},
		{
			name: "age below min",
			payload: map[string]interface{}{
				"age":   17.0,
				"name":  "Alice",
				"email": "alice@gmail.com",
				"score": 95.5,
			},
			wantErr: true,
		},
		{
			name: "email invalid regex",
			payload: map[string]interface{}{
				"age":   30.0,
				"name":  "Alice",
				"email": "alice-invalid-email",
				"score": 95.5,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := models.Record{Payload: tt.payload}
			err := validateRecord(rec, rules)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTransformation(t *testing.T) {
	transforms := []models.TransformConfig{
		{Type: "rename", Params: map[string]string{"from": "old_name", "to": "new_name"}},
		{Type: "cast", Params: map[string]string{"field": "score", "to_type": "integer"}},
		{Type: "default", Params: map[string]string{"field": "country", "value": "Unknown"}},
		{Type: "math", Params: map[string]string{"field": "val", "multiplier": "2.5", "to_field": "val_multiplied"}},
	}

	payload := map[string]interface{}{
		"old_name": "Alice",
		"score":    "95.6",
		"val":      10.0,
	}

	rec := models.Record{Payload: payload}
	txRec, err := transformRecord(rec, transforms)
	if err != nil {
		t.Fatalf("transformRecord failed: %v", err)
	}

	if _, exists := txRec.Payload["old_name"]; exists {
		t.Error("expected old_name to be renamed")
	}
	if txRec.Payload["new_name"] != "Alice" {
		t.Errorf("expected new_name = Alice, got %v", txRec.Payload["new_name"])
	}
	if txRec.Payload["score"] != int64(95) {
		t.Errorf("expected score = 95, got %v (%T)", txRec.Payload["score"], txRec.Payload["score"])
	}
	if txRec.Payload["country"] != "Unknown" {
		t.Errorf("expected country default = Unknown, got %v", txRec.Payload["country"])
	}
	if txRec.Payload["val_multiplied"] != 25.0 {
		t.Errorf("expected val_multiplied = 25.0, got %v", txRec.Payload["val_multiplied"])
	}
}

func TestPipeline_EndToEnd(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pipeline-e2e-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Mock CSV File
	csvContent := "country,cases,deaths\nUSA,100,5\nUSA,200,10\nCanada,50,2\n"
	csvPath := filepath.Join(tmpDir, "data.csv")
	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	// Mock JSON File
	jsonContent := `[
		{"country": "USA", "cases": 150, "deaths": 4},
		{"country": "Canada", "cases": 80, "deaths": 3}
	]`
	jsonPath := filepath.Join(tmpDir, "data.json")
	if err := os.WriteFile(jsonPath, []byte(jsonContent), 0644); err != nil {
		t.Fatalf("failed to write json: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	s, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	jobID := "e2e-job-1"

	spec := models.JobSpec{
		Name: "E2E COVID Aggregation",
		Sources: []models.SourceConfig{
			{ID: "src-csv", Type: "csv", Path: csvPath},
			{ID: "src-json", Type: "json", Path: jsonPath},
		},
		Validation: models.ValidationConfig{
			Rules: []models.ValidationRule{
				{Field: "cases", Rule: "required"},
			},
		},
		Transformations: []models.TransformConfig{
			{Type: "cast", Params: map[string]string{"field": "cases", "to_type": "integer"}},
			{Type: "cast", Params: map[string]string{"field": "deaths", "to_type": "integer"}},
		},
		Aggregation: &models.AggregationConfig{
			GroupBy: []string{"country"},
			Metrics: []models.AggregationMetric{
				{Field: "cases", Type: "sum", OutputField: "total_cases"},
				{Field: "deaths", Type: "avg", OutputField: "avg_deaths"},
			},
		},
		Exports: []models.ExportConfig{
			{Type: "sqlite", Params: map[string]string{"table": "e2e_results"}},
			{Type: "json", Params: map[string]string{"path": filepath.Join(tmpDir, "out.json")}},
		},
	}

	job := &models.Job{
		ID:        jobID,
		Name:      spec.Name,
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
		Spec:      spec,
	}

	if err := s.CreateJob(ctx, job); err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	pipe := NewPipeline(ctx, jobID, spec, s)
	err = pipe.Run()
	if err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// 1. Verify job finished as completed
	fJob, err := s.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if fJob.Status != models.StatusCompleted {
		t.Errorf("expected job status completed, got %s", fJob.Status)
	}

	// 2. Verify metrics
	m, err := s.GetJobMetrics(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get metrics: %v", err)
	}
	if m.RecordsIngested != 5 {
		t.Errorf("expected 5 ingested, got %d", m.RecordsIngested)
	}
	if m.RecordsExported != 2 {
		t.Errorf("expected 2 exported, got %d", m.RecordsExported)
	}

	// 3. Verify SQLite results
	results, err := s.GetJobResults(ctx, jobID)
	if err != nil {
		t.Fatalf("failed to get results: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 aggregated result groups, got %d", len(results))
	}

	var usaCases float64
	var canCases float64
	for _, r := range results {
		cName := r.ResultData["country"].(string)
		if cName == "USA" {
			usaCases = r.ResultData["total_cases"].(float64)
		} else if cName == "Canada" {
			canCases = r.ResultData["total_cases"].(float64)
		}
	}

	if usaCases != 450 {
		t.Errorf("expected USA cases 450, got %f", usaCases)
	}
	if canCases != 130 {
		t.Errorf("expected Canada cases 130, got %f", canCases)
	}
}
