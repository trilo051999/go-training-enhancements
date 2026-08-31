package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
	"github.com/trilochanparida/go-pipeline/internal/models"
)

type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore initializes a new SQLite connection, sets up tables, and optimizes settings
func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite DB: %w", err)
	}

	// Optimize SQLite for concurrent reading and single-threaded writing
	db.SetMaxOpenConns(1)

	s := &SQLiteStore{db: db}
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return s, nil
}

// Close closes the database connection
func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) initSchema() error {
	queries := []string{
		`PRAGMA foreign_keys = ON;`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			status TEXT NOT NULL,
			spec TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			started_at DATETIME,
			ended_at DATETIME
		);`,
		`CREATE TABLE IF NOT EXISTS job_metrics (
			job_id TEXT PRIMARY KEY,
			records_ingested INTEGER DEFAULT 0,
			records_validated INTEGER DEFAULT 0,
			records_transformed INTEGER DEFAULT 0,
			records_aggregated INTEGER DEFAULT 0,
			records_exported INTEGER DEFAULT 0,
			records_failed INTEGER DEFAULT 0,
			processing_rate_eps REAL DEFAULT 0.0,
			current_stage TEXT,
			updated_at DATETIME NOT NULL,
			FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS job_errors (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id TEXT NOT NULL,
			record_id TEXT,
			stage TEXT NOT NULL,
			error_message TEXT NOT NULL,
			raw_record TEXT,
			created_at DATETIME NOT NULL,
			FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS job_results (
			job_id TEXT NOT NULL,
			group_key TEXT NOT NULL,
			result_data TEXT NOT NULL,
			PRIMARY KEY(job_id, group_key),
			FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
		);`,
	}

	for _, query := range queries {
		if _, err := s.db.Exec(query); err != nil {
			return fmt.Errorf("schema error: %s: %w", query, err)
		}
	}
	return nil
}

func (s *SQLiteStore) CreateJob(ctx context.Context, job *models.Job) error {
	specBytes, err := json.Marshal(job.Spec)
	if err != nil {
		return fmt.Errorf("failed to marshal job spec: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`INSERT INTO jobs (id, name, status, spec, created_at, started_at, ended_at) 
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.Name, string(job.Status), string(specBytes), job.CreatedAt, job.StartedAt, job.EndedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_metrics (job_id, updated_at) VALUES (?, ?)`,
		job.ID, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert initial metrics: %w", err)
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetJob(ctx context.Context, id string) (*models.Job, error) {
	var (
		name, statusStr, specStr string
		createdAt                time.Time
		startedAt, endedAt       *time.Time
	)

	err := s.db.QueryRowContext(ctx,
		`SELECT name, status, spec, created_at, started_at, ended_at FROM jobs WHERE id = ?`,
		id,
	).Scan(&name, &statusStr, &specStr, &createdAt, &startedAt, &endedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job not found: %s", id)
	} else if err != nil {
		return nil, err
	}

	var spec models.JobSpec
	if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job spec: %w", err)
	}

	return &models.Job{
		ID:        id,
		Name:      name,
		Status:    models.JobStatus(statusStr),
		Spec:      spec,
		CreatedAt: createdAt,
		StartedAt: startedAt,
		EndedAt:   endedAt,
	}, nil
}

func (s *SQLiteStore) ListJobs(ctx context.Context) ([]*models.Job, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, status, spec, created_at, started_at, ended_at FROM jobs ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*models.Job
	for rows.Next() {
		var (
			id, name, statusStr, specStr string
			createdAt                    time.Time
			startedAt, endedAt           *time.Time
		)
		if err := rows.Scan(&id, &name, &statusStr, &specStr, &createdAt, &startedAt, &endedAt); err != nil {
			return nil, err
		}

		var spec models.JobSpec
		if err := json.Unmarshal([]byte(specStr), &spec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal job spec: %w", err)
		}

		jobs = append(jobs, &models.Job{
			ID:        id,
			Name:      name,
			Status:    models.JobStatus(statusStr),
			Spec:      spec,
			CreatedAt: createdAt,
			StartedAt: startedAt,
			EndedAt:   endedAt,
		})
	}
	return jobs, nil
}

func (s *SQLiteStore) UpdateJobStatus(ctx context.Context, id string, status models.JobStatus) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status = ? WHERE id = ?`, string(status), id)
	return err
}

func (s *SQLiteStore) UpdateJobTimes(ctx context.Context, id string, start bool) error {
	now := time.Now()
	var query string
	if start {
		query = `UPDATE jobs SET started_at = ? WHERE id = ?`
	} else {
		query = `UPDATE jobs SET ended_at = ? WHERE id = ?`
	}
	_, err := s.db.ExecContext(ctx, query, now, id)
	return err
}

func (s *SQLiteStore) DeleteJob(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) GetJobMetrics(ctx context.Context, jobID string) (*models.JobMetrics, error) {
	var m models.JobMetrics
	m.JobID = jobID
	var currentStage sql.NullString

	err := s.db.QueryRowContext(ctx,
		`SELECT records_ingested, records_validated, records_transformed, records_aggregated, 
		        records_exported, records_failed, processing_rate_eps, current_stage, updated_at 
		 FROM job_metrics WHERE job_id = ?`,
		jobID,
	).Scan(
		&m.RecordsIngested, &m.RecordsValidated, &m.RecordsTransformed, &m.RecordsAggregated,
		&m.RecordsExported, &m.RecordsFailed, &m.ProcessingRateEPS, &currentStage, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("metrics not found for job: %s", jobID)
	} else if err != nil {
		return nil, err
	}

	if currentStage.Valid {
		m.CurrentStage = currentStage.String
	}

	return &m, nil
}

func (s *SQLiteStore) UpdateJobMetrics(ctx context.Context, m *models.JobMetrics) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE job_metrics SET 
			records_ingested = ?, 
			records_validated = ?, 
			records_transformed = ?, 
			records_aggregated = ?, 
			records_exported = ?, 
			records_failed = ?, 
			processing_rate_eps = ?, 
			current_stage = ?, 
			updated_at = ?
		 WHERE job_id = ?`,
		m.RecordsIngested, m.RecordsValidated, m.RecordsTransformed, m.RecordsAggregated,
		m.RecordsExported, m.RecordsFailed, m.ProcessingRateEPS, m.CurrentStage, time.Now(), m.JobID,
	)
	return err
}

func (s *SQLiteStore) AddJobError(ctx context.Context, jobErr *models.JobError) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO job_errors (job_id, record_id, stage, error_message, raw_record, created_at) 
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobErr.JobID, jobErr.RecordID, jobErr.Stage, jobErr.ErrorMessage, jobErr.RawRecord, time.Now(),
	)
	return err
}

func (s *SQLiteStore) GetJobErrors(ctx context.Context, jobID string) ([]*models.JobError, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, record_id, stage, error_message, raw_record, created_at 
		 FROM job_errors WHERE job_id = ? ORDER BY created_at ASC`,
		jobID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var errors []*models.JobError
	for rows.Next() {
		var (
			id                   int64
			recordID, rawRecord sql.NullString
			stage, errorMessage  string
			createdAt            time.Time
		)
		if err := rows.Scan(&id, &recordID, &stage, &errorMessage, &rawRecord, &createdAt); err != nil {
			return nil, err
		}

		jobErr := &models.JobError{
			ID:           id,
			JobID:        jobID,
			Stage:        stage,
			ErrorMessage: errorMessage,
			CreatedAt:    createdAt,
		}
		if recordID.Valid {
			jobErr.RecordID = recordID.String
		}
		if rawRecord.Valid {
			jobErr.RawRecord = rawRecord.String
		}

		errors = append(errors, jobErr)
	}
	return errors, nil
}

func (s *SQLiteStore) SaveJobResult(ctx context.Context, jobID string, groupKey string, resultData map[string]interface{}) error {
	dataBytes, err := json.Marshal(resultData)
	if err != nil {
		return fmt.Errorf("failed to marshal result data: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO job_results (job_id, group_key, result_data) VALUES (?, ?, ?)`,
		jobID, groupKey, string(dataBytes),
	)
	return err
}

func (s *SQLiteStore) GetJobResults(ctx context.Context, jobID string) ([]ResultRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT group_key, result_data FROM job_results WHERE job_id = ?`,
		jobID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ResultRow
	for rows.Next() {
		var groupKey, resultDataStr string
		if err := rows.Scan(&groupKey, &resultDataStr); err != nil {
			return nil, err
		}

		var resultData map[string]interface{}
		if err := json.Unmarshal([]byte(resultDataStr), &resultData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal result data: %w", err)
		}

		results = append(results, ResultRow{
			GroupKey:   groupKey,
			ResultData: resultData,
		})
	}
	return results, nil
}
