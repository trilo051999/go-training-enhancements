package api

import (
	"log"
	"net/http"
	"time"
)

// LoggingMiddleware logs incoming requests with HTTP method, path, and duration
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

// NewRouter registers the REST API endpoints and returns the configured HTTP handler
func NewRouter(h *Handler) http.Handler {
	mux := http.NewServeMux()

	// REST API Job Management & Observability Endpoints
	mux.HandleFunc("POST /api/v1/pipelines", h.CreatePipeline)
	mux.HandleFunc("GET /api/v1/pipelines", h.ListPipelines)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", h.GetPipeline)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/progress", h.GetProgress)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/results", h.GetResults)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/errors", h.GetErrors)
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", h.CancelPipeline)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", h.DeletePipeline)

	// Observability & Health endpoints
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"status": "up",
			"system": "pipeline-engine",
		})
	})

	return LoggingMiddleware(mux)
}
