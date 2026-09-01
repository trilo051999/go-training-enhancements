package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/pipeline"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

func setupTestRouter(t *testing.T) (http.Handler, store.Store, func()) {
	tmpDir, err := os.MkdirTemp("", "api-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	s, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}

	mgr := pipeline.NewManager(s)
	handler := NewHandler(mgr)
	router := NewRouter(handler)

	cleanup := func() {
		_ = s.Close()
		_ = os.RemoveAll(tmpDir)
	}

	return router, s, cleanup
}

func TestAPI_Lifecycle(t *testing.T) {
	router, s, cleanup := setupTestRouter(t)
	defer cleanup()

	// 1. Create a dummy file source
	tmpDir, _ := os.MkdirTemp("", "api-data-*")
	defer os.RemoveAll(tmpDir)
	sampleCSV := filepath.Join(tmpDir, "users.csv")
	_ = os.WriteFile(sampleCSV, []byte("name,age\nAlice,30\nBob,15\nCharlie,40\n"), 0644)

	spec := models.JobSpec{
		Name: "Test API Pipeline",
		Sources: []models.SourceConfig{
			{ID: "csv-1", Type: "csv", Path: sampleCSV},
		},
		Validation: models.ValidationConfig{
			Rules: []models.ValidationRule{
				{Field: "age", Rule: "min", Value: 18.0}, // Bob (15) will fail validation
			},
		},
		Transformations: []models.TransformConfig{
			{Type: "cast", Params: map[string]string{"field": "age", "to_type": "integer"}},
		},
		Aggregation: &models.AggregationConfig{
			GroupBy: []string{"name"},
			Metrics: []models.AggregationMetric{
				{Field: "age", Type: "sum", OutputField: "sum_age"},
			},
		},
	}

	body, _ := json.Marshal(spec)

	// Step A: POST /api/v1/pipelines
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var createResp struct {
		Success bool        `json:"success"`
		Data    *models.Job `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("failed to decode create response: %v", err)
	}

	jobID := createResp.Data.ID
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}

	// Wait briefly for pipeline processing
	time.Sleep(100 * time.Millisecond)

	// Step B: GET /api/v1/pipelines
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Step C: GET /api/v1/pipelines/{id}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID, nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Step D: GET /api/v1/pipelines/{id}/progress
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/progress", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Step E: GET /api/v1/pipelines/{id}/results
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/results", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Step F: GET /api/v1/pipelines/{id}/errors
	req = httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+jobID+"/errors", nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	var errResp struct {
		Success bool               `json:"success"`
		Data    []*models.JobError `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &errResp)
	if len(errResp.Data) == 0 {
		t.Errorf("expected validation errors for Bob (15), got none")
	}

	// Step G: DELETE /api/v1/pipelines/{id}
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/"+jobID, nil)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// Ensure job is deleted
	_, err := s.GetJob(context.Background(), jobID)
	if err == nil {
		t.Error("expected job to be deleted from store, but was found")
	}
}
