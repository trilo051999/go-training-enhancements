package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/pipeline"
)

type Handler struct {
	manager *pipeline.Manager
}

func NewHandler(m *pipeline.Manager) *Handler {
	return &Handler{manager: m}
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, APIResponse{
		Success: false,
		Error:   msg,
	})
}

// CreatePipeline handles POST /api/v1/pipelines
func (h *Handler) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	var spec models.JobSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	if len(spec.Sources) == 0 {
		writeError(w, http.StatusBadRequest, "at least one source definition is required")
		return
	}

	job, err := h.manager.CreateJob(r.Context(), spec)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to create pipeline: %v", err))
		return
	}

	writeJSON(w, http.StatusAccepted, APIResponse{
		Success: true,
		Data:    job,
	})
}

// ListPipelines handles GET /api/v1/pipelines
func (h *Handler) ListPipelines(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.manager.GetStore().ListJobs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to list pipelines: %v", err))
		return
	}

	if jobs == nil {
		jobs = []*models.Job{}
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    jobs,
	})
}

// GetPipeline handles GET /api/v1/pipelines/{id}
func (h *Handler) GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	job, err := h.manager.GetStore().GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", id))
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    job,
	})
}

type ProgressResponse struct {
	JobID             string             `json:"job_id"`
	Status            models.JobStatus   `json:"status"`
	PercentComplete   float64            `json:"percent_complete"`
	RecordsProcessed  int64              `json:"records_processed"`
	RecordsFailed     int64              `json:"records_failed"`
	ProcessingRateEPS float64            `json:"processing_rate_eps"`
	CurrentStage      string             `json:"current_stage"`
	StartedAt         string             `json:"started_at,omitempty"`
	EndedAt           string             `json:"ended_at,omitempty"`
	Metrics           *models.JobMetrics `json:"metrics,omitempty"`
}

// GetProgress handles GET /api/v1/pipelines/{id}/progress
func (h *Handler) GetProgress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	job, err := h.manager.GetStore().GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", id))
		return
	}

	metrics, err := h.manager.GetStore().GetJobMetrics(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get metrics: %v", err))
		return
	}

	var percent float64
	switch job.Status {
	case models.StatusCompleted:
		percent = 100.0
	case models.StatusPending:
		percent = 0.0
	case models.StatusRunning:
		if metrics.RecordsIngested > 0 {
			percent = float64(metrics.RecordsExported+metrics.RecordsFailed) / float64(metrics.RecordsIngested) * 100.0
			if percent > 99.0 {
				percent = 99.0
			}
		} else {
			percent = 10.0
		}
	default:
		percent = 100.0
	}

	resp := ProgressResponse{
		JobID:             job.ID,
		Status:            job.Status,
		PercentComplete:   percent,
		RecordsProcessed:  metrics.RecordsTransformed,
		RecordsFailed:     metrics.RecordsFailed,
		ProcessingRateEPS: metrics.ProcessingRateEPS,
		CurrentStage:      metrics.CurrentStage,
		Metrics:           metrics,
	}
	if job.StartedAt != nil {
		resp.StartedAt = job.StartedAt.Format(time.RFC3339)
	}
	if job.EndedAt != nil {
		resp.EndedAt = job.EndedAt.Format(time.RFC3339)
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    resp,
	})
}

// GetResults handles GET /api/v1/pipelines/{id}/results
func (h *Handler) GetResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	job, err := h.manager.GetStore().GetJob(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("job not found: %s", id))
		return
	}

	results, err := h.manager.GetStore().GetJobResults(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get results: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"job_id":  id,
			"exports": job.Spec.Exports,
			"results": results,
		},
	})
}

// GetErrors handles GET /api/v1/pipelines/{id}/errors
func (h *Handler) GetErrors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	jobErrors, err := h.manager.GetStore().GetJobErrors(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get errors: %v", err))
		return
	}

	if jobErrors == nil {
		jobErrors = []*models.JobError{}
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    jobErrors,
	})
}

// CancelPipeline handles PATCH /api/v1/pipelines/{id}/cancel
func (h *Handler) CancelPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	if err := h.manager.CancelJob(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to cancel pipeline: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"message": "job cancellation initiated", "job_id": id},
	})
}

// DeletePipeline handles DELETE /api/v1/pipelines/{id}
func (h *Handler) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "job ID is required")
		return
	}

	if err := h.manager.DeleteJob(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete pipeline: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Data:    map[string]string{"message": "job and artifacts deleted", "job_id": id},
	})
}
