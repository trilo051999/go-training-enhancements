package models

import "time"

type JobError struct {
	ID           int64     `json:"id"`
	JobID        string    `json:"job_id"`
	RecordID     string    `json:"record_id,omitempty"`
	Stage        string    `json:"stage"` // "ingest", "validate", "transform", "aggregate", "export"
	ErrorMessage string    `json:"error_message"`
	RawRecord    string    `json:"raw_record,omitempty"` // JSON string of the failed record
	CreatedAt    time.Time `json:"created_at"`
}
