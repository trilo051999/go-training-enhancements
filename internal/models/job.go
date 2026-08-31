package models

import (
	"time"
)

type JobStatus string

const (
	StatusPending   JobStatus = "pending"
	StatusRunning   JobStatus = "running"
	StatusCompleted JobStatus = "completed"
	StatusFailed    JobStatus = "failed"
	StatusCancelled JobStatus = "cancelled"
)

type JobSpec struct {
	Name            string               `json:"name"`
	Sources         []SourceConfig       `json:"sources"`
	Validation      ValidationConfig     `json:"validation"`
	Transformations []TransformConfig    `json:"transformations"`
	Aggregation     *AggregationConfig   `json:"aggregation,omitempty"`
	Exports         []ExportConfig       `json:"exports"`
	Concurrency     *ConcurrencyConfig   `json:"concurrency,omitempty"`
}

type SourceConfig struct {
	ID     string            `json:"id"`
	Type   string            `json:"type"`             // "csv", "json", "api"
	Path   string            `json:"path"`             // File path or URL
	Params map[string]string `json:"params,omitempty"` // e.g. delimiter, headers, query params
}

type ValidationRule struct {
	Field string      `json:"field"`
	Rule  string      `json:"rule"`  // "required", "min", "max", "regex", "type"
	Value interface{} `json:"value"` // Threshold, regex pattern, or type name
}

type ValidationConfig struct {
	Rules []ValidationRule `json:"rules"`
}

type TransformConfig struct {
	Type   string            `json:"type"`   // "rename", "cast", "math", "default"
	Params map[string]string `json:"params"` // e.g. {"from": "old", "to": "new"}
}

type AggregationMetric struct {
	Field       string `json:"field"`
	Type        string `json:"type"` // "sum", "count", "avg", "min", "max"
	OutputField string `json:"output_field"`
}

type AggregationConfig struct {
	GroupBy []string            `json:"group_by"`
	Metrics []AggregationMetric `json:"metrics"`
}

type ExportConfig struct {
	Type   string            `json:"type"`   // "sqlite", "csv", "json"
	Params map[string]string `json:"params"` // e.g. {"table": "my_table"} or {"path": "out.csv"}
}

type ConcurrencyConfig struct {
	ValidationWorkers int `json:"validation_workers"`
	TransformWorkers  int `json:"transform_workers"`
	BufferSize        int `json:"buffer_size"`
}

type Job struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Status    JobStatus  `json:"status"`
	Spec      JobSpec    `json:"spec"`
	CreatedAt time.Time  `json:"created_at"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type JobMetrics struct {
	JobID              string    `json:"job_id"`
	RecordsIngested    int64     `json:"records_ingested"`
	RecordsValidated   int64     `json:"records_validated"`
	RecordsTransformed int64     `json:"records_transformed"`
	RecordsAggregated  int64     `json:"records_aggregated"`
	RecordsExported    int64     `json:"records_exported"`
	RecordsFailed      int64     `json:"records_failed"`
	ProcessingRateEPS  float64   `json:"processing_rate_eps"`
	CurrentStage       string    `json:"current_stage,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
}
