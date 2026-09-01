# Engineering Report: Concurrent Data Processing Pipeline

## 1. Executive Summary & Design Choices

The goal of this project is to build a robust, concurrent, multi-stage data processing pipeline in Go capable of ingesting diverse data sources, validating and transforming records in parallel, computing grouped aggregations, and persisting results with full observability and REST management.

### Key Architectural Decisions
- **Stage Isolation & Single Responsibility**: The pipeline is partitioned into distinct stages (`Ingest` ➔ `Validate` ➔ `Transform` ➔ `Aggregate` ➔ `Export`). Each stage encapsulates its own logic, error handling, and worker pool.
- **Dynamic Schema Flexibility**: Records are represented using a flexible `Record` model with `map[string]interface{}` payloads, enabling ingestion of arbitrary CSV columns, JSON objects, and external REST API payloads without requiring hardcoded schemas.
- **Pure-Go SQLite Storage**: Selected `modernc.org/sqlite` over CGO-based drivers to eliminate native C compiler dependencies, ensuring cross-platform portability across macOS, Linux, and containerized Docker environments.
- **Standard Library Routing**: Leveraged Go 1.22+ enhanced `http.ServeMux` for REST routing (e.g. `POST /api/v1/pipelines/{id}`), removing heavy third-party framework overhead while maintaining idiomatic Go code.

---

## 2. Concurrency & Synchronization Model

- **Worker Pools (Fan-Out / Fan-In)**: The compute-heavy `Validation` and `Transformation` stages utilize configurable worker pools (`sync.WaitGroup`). Channels safely distribute records to workers and funnels them to subsequent stages.
- **Channel-Driven Flow Control**: Bounded channels pass typed records between stages. Bounded buffers prevent out-of-memory errors by applying backpressure upstream if the export target experiences high latency.
- **Cascading Channel Closures**: Upstream wait groups signal stage completion and systematically close downstream channels, ensuring no goroutines or channels leak.
- **Centralized Error & Progress Side-Channels**:
  - `errorCh`: Dedicated worker consumes validation and processing errors asynchronously and writes them to SQLite without stalling data workers.
  - **Lock-Free Atomic Metrics**: High-throughput atomic operations (`sync/atomic`) record throughput metrics in memory, which are periodically flushed to the database by a background ticker, preventing database write-lock contention.
- **Context Cancellation**: A root `context.Context` coordinates all worker routines, HTTP downloads, and DB queries. Triggering `Cancel()` immediately terminates active requests, drains channels, and sets the job to a `cancelled` state.

---

## 3. Trade-offs & Scaling Considerations

| Design Choice | Benefits | Trade-offs & Mitigation |
| :--- | :--- | :--- |
| **In-Memory Streaming Aggregator** | Sub-millisecond aggregation speed; zero external dependencies. | Large datasets with high cardinality group keys may require significant RAM. *Mitigation: Group state can be backed by disk or external Redis store for huge keyspaces.* |
| **Embedded SQLite Database** | Zero external infrastructure needed; self-contained migrations. | Single-threaded write model limits extreme write concurrency. *Mitigation: Used atomic batching and memory-buffered progress flushes; architecture allows drop-in PostgreSQL adapter.* |
| **Dynamic Schema Type Casting** | Ingests arbitrary CSV/JSON inputs dynamically. | Reflection / runtime type assertions introduce minor CPU overhead compared to code-generated structs. *Mitigation: High-performance type switch helpers.* |
