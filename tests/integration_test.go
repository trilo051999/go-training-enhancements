package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/api"
	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/pipeline"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

func TestEndToEndPipelineWithHTTPAndFiles(t *testing.T) {
	// 1. Start a Mock HTTP Server providing remote JSON data
	mockAPIServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		users := []map[string]interface{}{
			{"id": 1, "country": "India", "cases": 300, "city": "Bengaluru"},
			{"id": 2, "country": "India", "cases": 200, "city": "Mumbai"},
			{"id": 3, "country": "InvalidCountry", "cases": -50, "city": "Unknown"}, // Validation error: negative cases
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(users)
	}))
	defer mockAPIServer.Close()

	// 2. Create local CSV and JSON source files
	tmpDir, err := os.MkdirTemp("", "e2e-pipeline-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	csvFile := filepath.Join(tmpDir, "data.csv")
	_ = os.WriteFile(csvFile, []byte("country,cases,city\nUSA,500,New York\nUSA,300,Chicago\nCanada,250,Toronto\n"), 0644)

	jsonFile := filepath.Join(tmpDir, "data.json")
	_ = os.WriteFile(jsonFile, []byte(`[
		{"country": "Canada", "cases": 150, "city": "Vancouver"},
		{"country": "UK", "cases": 400, "city": "London"}
	]`), 0644)

	// 3. Initialize SQLite DB and Server
	dbPath := filepath.Join(tmpDir, "pipeline.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create db store: %v", err)
	}
	defer dbStore.Close()

	mgr := pipeline.NewManager(dbStore)
	handler := api.NewHandler(mgr)
	router := api.NewRouter(handler)

	outJSON := filepath.Join(tmpDir, "exports", "summary.json")
	outCSV := filepath.Join(tmpDir, "exports", "summary.csv")

	// 4. Create Job via POST /api/v1/pipelines
	spec := models.JobSpec{
		Name: "Global Pandemic Aggregation Job",
		Sources: []models.SourceConfig{
			{ID: "http-api", Type: "api", Path: mockAPIServer.URL},
			{ID: "local-csv", Type: "csv", Path: csvFile},
			{ID: "local-json", Type: "json", Path: jsonFile},
		},
		Validation: models.ValidationConfig{
			Rules: []models.ValidationRule{
				{Field: "cases", Rule: "required"},
				{Field: "cases", Rule: "min", Value: 0.0}, // Filters negative cases
			},
		},
		Transformations: []models.TransformConfig{
			{Type: "cast", Params: map[string]string{"field": "cases", "to_type": "integer"}},
		},
		Aggregation: &models.AggregationConfig{
			GroupBy: []string{"country"},
			Metrics: []models.AggregationMetric{
				{Field: "cases", Type: "sum", OutputField: "total_cases"},
				{Field: "cases", Type: "count", OutputField: "report_count"},
				{Field: "cases", Type: "avg", OutputField: "avg_cases"},
			},
		},
		Exports: []models.ExportConfig{
			{Type: "sqlite", Params: map[string]string{"table": "global_summary"}},
			{Type: "json", Params: map[string]string{"path": outJSON}},
			{Type: "csv", Params: map[string]string{"path": outCSV}},
		},
		Concurrency: &models.ConcurrencyConfig{
			ValidationWorkers: 2,
			TransformWorkers:  2,
			BufferSize:        50,
		},
	}

	body, _ := json.Marshal(spec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected create status: %d (%s)", rec.Code, rec.Body.String())
	}

	var createResp struct {
		Data *models.Job `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	jobID := createResp.Data.ID

	// 5. Poll progress until finished
	var finishedJob *models.Job
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		job, err := dbStore.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatalf("failed to fetch job: %v", err)
		}
		if job.Status == models.StatusCompleted || job.Status == models.StatusFailed {
			finishedJob = job
			break
		}
	}

	if finishedJob == nil || finishedJob.Status != models.StatusCompleted {
		t.Fatalf("job did not complete successfully in time, status: %v", finishedJob)
	}

	// 6. Verify Aggregated SQLite Results
	results, err := dbStore.GetJobResults(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get results: %v", err)
	}

	countryCases := make(map[string]float64)
	for _, r := range results {
		c := r.ResultData["country"].(string)
		countryCases[c] = r.ResultData["total_cases"].(float64)
	}

	// India: 300 + 200 = 500
	if countryCases["India"] != 500 {
		t.Errorf("expected India cases 500, got %f", countryCases["India"])
	}
	// USA: 500 + 300 = 800
	if countryCases["USA"] != 800 {
		t.Errorf("expected USA cases 800, got %f", countryCases["USA"])
	}
	// Canada: 250 + 150 = 400
	if countryCases["Canada"] != 400 {
		t.Errorf("expected Canada cases 400, got %f", countryCases["Canada"])
	}

	// 7. Verify Exported Files Exist and Contain Data
	if _, err := os.Stat(outJSON); os.IsNotExist(err) {
		t.Errorf("expected exported JSON file %s to exist", outJSON)
	}
	if _, err := os.Stat(outCSV); os.IsNotExist(err) {
		t.Errorf("expected exported CSV file %s to exist", outCSV)
	}

	// 8. Verify Error Logs for Invalid Record
	errors, err := dbStore.GetJobErrors(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to get errors: %v", err)
	}
	if len(errors) != 1 {
		t.Errorf("expected 1 error logged for negative cases, got %d", len(errors))
	}
}

func TestPipelineCancellation(t *testing.T) {
	// Mock an infinite/slow HTTP API
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sleep to simulate long stream
		time.Sleep(5 * time.Second)
		fmt.Fprintf(w, `[{"id": 1}]`)
	}))
	defer slowServer.Close()

	tmpDir, _ := os.MkdirTemp("", "cancel-test-*")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "pipeline.db")
	dbStore, _ := store.NewSQLiteStore(dbPath)
	defer dbStore.Close()

	mgr := pipeline.NewManager(dbStore)
	handler := api.NewHandler(mgr)
	router := api.NewRouter(handler)

	spec := models.JobSpec{
		Name: "Long Running Cancellation Test",
		Sources: []models.SourceConfig{
			{ID: "slow-api", Type: "api", Path: slowServer.URL},
		},
	}

	body, _ := json.Marshal(spec)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var createResp struct {
		Data *models.Job `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	jobID := createResp.Data.ID

	// Cancel the job immediately via PATCH /api/v1/pipelines/{id}/cancel
	time.Sleep(50 * time.Millisecond)
	cancelReq := httptest.NewRequest(http.MethodPatch, "/api/v1/pipelines/"+jobID+"/cancel", nil)
	cancelRec := httptest.NewRecorder()
	router.ServeHTTP(cancelRec, cancelReq)

	if cancelRec.Code != http.StatusOK {
		t.Fatalf("expected cancel status 200, got %d", cancelRec.Code)
	}

	// Verify job is marked as cancelled
	time.Sleep(50 * time.Millisecond)
	job, err := dbStore.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("failed to fetch job: %v", err)
	}
	if job.Status != models.StatusCancelled {
		t.Errorf("expected status cancelled, got %s", job.Status)
	}
}
