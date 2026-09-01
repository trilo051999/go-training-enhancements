# Golang Concurrent Data Processing Pipeline

A robust, concurrent, multi-stage data processing pipeline built in Go. It ingests data from heterogeneous sources (CSV files, JSON files, external HTTP REST APIs), validates and transforms records in parallel worker pools, computes running grouped aggregations, exports results to SQLite and file stores (CSV/JSON), and exposes a REST API for pipeline management and real-time observability.

---

## Table of Contents
- [Architecture & Design](#architecture--design)
- [Concurrency Model](#concurrency-model)
- [REST API Reference](#rest-api-reference)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Building and Running](#building-and-running)
  - [Running Tests](#running-tests)
- [Sample Job Execution](#sample-job-execution)
- [Project Layout](#project-layout)

---

## Architecture & Design

The pipeline connects independent stages using Go channels and worker pools (fan-out / fan-in):

```
┌────────────────────────────────────────────────────────┐
│                   Source Ingestors                     │
│         (CSV Files / JSON Files / HTTP APIs)           │
└──────────────────────────┬─────────────────────────────┘
                           │ (recordsCh)
                           ▼
┌────────────────────────────────────────────────────────┐
│               Validation Stage (Worker Pool)           │
│     (Checks schema, bounds, regex, required fields)    │
└────────────┬─────────────────────────────┬─────────────┘
             │ (validatedCh)               │ (errorCh)
             ▼                             ▼
┌───────────────────────────┐  ┌─────────────────────────┐
│ Transformation Stage Pool │  │ Central Error Collector │
│  (Cast, Rename, Math)     │  │   (Persists to SQLite)  │
└────────────┬──────────────┘  └─────────────────────────┘
             │ (transformedCh)
             ▼
┌────────────────────────────────────────────────────────┐
│             Aggregation Stage (Fan-In Aggregator)      │
│      (Computes sums, counts, averages, min/max)        │
└──────────────────────────┬─────────────────────────────┘
                           │ (exportCh)
                           ▼
┌────────────────────────────────────────────────────────┐
│                      Export Stage                      │
│        (SQLite Dynamic Tables / JSON Files / CSV Files)│
└────────────────────────────────────────────────────────┘
```

---

## Concurrency Model

1. **Ingestion**: Each data source (file or remote URL) executes concurrently in its own goroutine and streams parsed records onto `recordsCh`.
2. **Validation (Fan-Out)**: A pool of `N` worker goroutines consumes from `recordsCh`, verifies business validation rules, routes valid records to `validatedCh`, and sends failed records to `errorCh`.
3. **Transformation (Fan-Out)**: A pool of `M` worker goroutines mutates and standardizes records in parallel, forwarding transformed payloads to `transformedCh`.
4. **Aggregation (Fan-In)**: Collects transformed records, groups them dynamically by custom keys, updates running totals (`sum`, `avg`, `min`, `max`, `count`), and writes summaries to `exportCh`.
5. **Export**: Consumes aggregated summaries and writes to disk (`.csv`, `.json`) and creates custom SQLite tables dynamically.
6. **Error & Progress Side-Channels**:
   - `errorCh`: Non-blocking error collector that aggregates failures across all stages into SQLite.
   - Atomic Progress Counters & Background Ticker: Tracks real-time record rates (EPS), stage latencies, and completion percentages without lock contention.
7. **Graceful Cancellation**: `context.Context` cancellation propagates immediately across all worker pools, stopping upstream readers and draining channels cleanly without leaking goroutines.

---

## REST API Reference

All management endpoints are prefixed with `/api/v1`.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/v1/pipelines` | Create and trigger an asynchronous pipeline job |
| `GET` | `/api/v1/pipelines` | List all pipeline jobs |
| `GET` | `/api/v1/pipelines/:id` | Get details and configuration of a specific job |
| `GET` | `/api/v1/pipelines/:id/progress` | Get real-time metrics, records processed, and % completion |
| `GET` | `/api/v1/pipelines/:id/results` | Retrieve job results (and export references) |
| `GET` | `/api/v1/pipelines/:id/errors` | Retrieve failed records and stage error messages |
| `PATCH`| `/api/v1/pipelines/:id/cancel` | Cancel a running pipeline job |
| `DELETE`| `/api/v1/pipelines/:id` | Cancel job, delete DB records, and purge exported files |
| `GET` | `/health` | Service health status |
| `GET` | `/metrics` | System status metrics |

---

## Getting Started

### Prerequisites
- **Go**: Version 1.22+ (Uses standard library routing `http.ServeMux`)

### Building and Running

1. **Build the server binary**:
   ```bash
   go build -o server ./cmd/server
   ```

2. **Run the server**:
   ```bash
   ./server -port 8080 -db pipeline.db
   ```
   Or using `go run`:
   ```bash
   go run ./cmd/server/main.go
   ```

### Running Tests

Execute all unit and integration test suites:
```bash
go test -v ./...
```

---

## Sample Job Execution

### 1. Create a Pipeline Job
```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d @data/samples/sample_job.json
```

**Response (HTTP 202 Accepted):**
```json
{
  "success": true,
  "data": {
    "id": "e98e8b23-1d0b-419b-b5b4-d73111f185ef",
    "name": "Global Pandemic & Healthcare Processing Pipeline",
    "status": "pending",
    "created_at": "2026-09-01T10:30:00Z"
  }
}
```

### 2. Check Job Progress & Real-Time Metrics
```bash
curl http://localhost:8080/api/v1/pipelines/e98e8b23-1d0b-419b-b5b4-d73111f185ef/progress
```

**Response:**
```json
{
  "success": true,
  "data": {
    "job_id": "e98e8b23-1d0b-419b-b5b4-d73111f185ef",
    "status": "completed",
    "percent_complete": 100,
    "records_processed": 14,
    "records_failed": 1,
    "processing_rate_eps": 140.0,
    "current_stage": "finished"
  }
}
```

### 3. Retrieve Aggregated Results
```bash
curl http://localhost:8080/api/v1/pipelines/e98e8b23-1d0b-419b-b5b4-d73111f185ef/results
```

### 4. Inspect Errors
```bash
curl http://localhost:8080/api/v1/pipelines/e98e8b23-1d0b-419b-b5b4-d73111f185ef/errors
```

### 5. Cancel a Running Job
```bash
curl -X PATCH http://localhost:8080/api/v1/pipelines/e98e8b23-1d0b-419b-b5b4-d73111f185ef/cancel
```

---

## Project Layout

```
go-training/
├── cmd/
│   └── server/
│       └── main.go              # Server entry point & graceful shutdown
├── internal/
│   ├── api/
│   │   ├── handler.go           # REST API handlers
│   │   ├── handler_test.go      # Endpoint unit tests
│   │   ├── router.go            # Route definitions & logging middleware
│   │   └── server.go            # HTTP server configuration
│   ├── models/
│   │   ├── error.go             # JobError model
│   │   ├── job.go               # Job, JobSpec, JobMetrics, and Config models
│   │   └── record.go            # Pipeline Record model
│   ├── pipeline/
│   │   ├── coordinator.go       # Pipeline stage coordinator & progress engine
│   │   ├── manager.go           # Asynchronous job lifecycle manager
│   │   ├── pipeline_test.go     # Stage unit tests & E2E tests
│   │   ├── stage_aggregate.go   # Grouped aggregation engine
│   │   ├── stage_export.go      # CSV, JSON, and SQLite table exporter
│   │   ├── stage_ingest.go      # CSV, JSON, and HTTP API stream parsers
│   │   ├── stage_transform.go   # Transformation worker pool
│   │   └── stage_validate.go    # Validation worker pool
│   └── store/
│       ├── sqlite.go            # SQLite store implementation with dynamic tables
│       ├── sqlite_test.go       # Storage CRUD tests
│       └── store.go             # Store interface definition
├── data/
│   └── samples/
│       ├── covid_data.csv       # Sample CSV source dataset
│       ├── sample_job.json      # Sample JobSpec configuration
│       └── users.json           # Sample JSON source dataset
├── tests/
│   └── integration_test.go      # E2E integration tests with HTTP servers & files
├── REPORT.md                    # Architecture & design trade-offs report
├── go.mod
└── README.md
```
