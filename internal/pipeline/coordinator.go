package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

type Pipeline struct {
	jobID   string
	spec    models.JobSpec
	store   store.Store
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.RWMutex
	stage   string

	// Atomic counters for real-time progress metrics
	ingestedCount    int64
	validatedCount   int64
	transformedCount int64
	aggregatedCount  int64
	exportedCount    int64
	failedCount      int64
}

// NewPipeline creates and prepares a pipeline for execution
func NewPipeline(ctx context.Context, jobID string, spec models.JobSpec, s store.Store) *Pipeline {
	ctx, cancel := context.WithCancel(ctx)

	// Fallback/Default concurrency configurations
	if spec.Concurrency == nil {
		spec.Concurrency = &models.ConcurrencyConfig{
			ValidationWorkers: 4,
			TransformWorkers:  4,
			BufferSize:        100,
		}
	}
	if spec.Concurrency.ValidationWorkers <= 0 {
		spec.Concurrency.ValidationWorkers = 4
	}
	if spec.Concurrency.TransformWorkers <= 0 {
		spec.Concurrency.TransformWorkers = 4
	}
	if spec.Concurrency.BufferSize <= 0 {
		spec.Concurrency.BufferSize = 100
	}

	return &Pipeline{
		jobID:  jobID,
		spec:   spec,
		store:  s,
		ctx:    ctx,
		cancel: cancel,
		stage:  "initialized",
	}
}

// Cancel terminates all running workers in the pipeline
func (p *Pipeline) Cancel() {
	p.cancel()
}

func (p *Pipeline) setStage(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stage = s
}

func (p *Pipeline) getCurrentStage() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stage
}

// Run executes the full pipeline end-to-end synchronously
func (p *Pipeline) Run() error {
	p.setStage("starting")
	_ = p.store.UpdateJobStatus(p.ctx, p.jobID, models.StatusRunning)
	_ = p.store.UpdateJobTimes(p.ctx, p.jobID, true) // Sets started_at

	// Channels connecting the pipeline stages
	bufSize := p.spec.Concurrency.BufferSize
	recordsCh := make(chan models.Record, bufSize)
	validatedCh := make(chan models.Record, bufSize)
	transformedCh := make(chan models.Record, bufSize)
	exportCh := make(chan map[string]interface{}, bufSize)

	// Side-channel for centralized errors collection
	errCh := make(chan *models.JobError, bufSize)

	// Spawn progress tracker and error collector goroutines
	p.startProgressTracker()

	var errorCollectorWG sync.WaitGroup
	p.startErrorCollector(errCh, &errorCollectorWG)

	// Stage WaitGroups to gracefully manage cascading close events
	var ingestWG sync.WaitGroup
	var validateWG sync.WaitGroup
	var transformWG sync.WaitGroup

	// 1. Start Ingest Stage
	p.setStage("ingest")
	p.startIngestionStage(recordsCh, errCh, &ingestWG)

	// Closer for recordsCh
	go func() {
		ingestWG.Wait()
		close(recordsCh)
	}()

	// 2. Start Validation Stage
	p.setStage("validate")
	p.startValidationStage(recordsCh, validatedCh, errCh, &validateWG)

	// Closer for validatedCh
	go func() {
		validateWG.Wait()
		close(validatedCh)
	}()

	// 3. Start Transformation Stage
	p.setStage("transform")
	p.startTransformationStage(validatedCh, transformedCh, errCh, &transformWG)

	// Closer for transformedCh
	go func() {
		transformWG.Wait()
		close(transformedCh)
	}()

	// 4. Start Aggregation Stage
	p.setStage("aggregate")
	p.startAggregationStage(transformedCh, exportCh, errCh)

	// 5. Start Export Stage
	p.setStage("export")
	exportDone := p.startExportStage(exportCh, errCh)

	// Wait for the Export stage and all upstream worker pools to conclude
	<-exportDone
	ingestWG.Wait()
	validateWG.Wait()
	transformWG.Wait()

	// Gracefully shut down error collector
	close(errCh)
	errorCollectorWG.Wait()

	// Update job state depending on success/failure/cancellation
	finalStatus := models.StatusCompleted
	p.setStage("finished")

	select {
	case <-p.ctx.Done():
		finalStatus = models.StatusCancelled
		p.setStage("cancelled")
	default:
		// Check if the entire ingestion failed and all records errored out
		if atomic.LoadInt64(&p.ingestedCount) == 0 && atomic.LoadInt64(&p.failedCount) > 0 {
			finalStatus = models.StatusFailed
			p.setStage("failed")
		}
	}

	_ = p.store.UpdateJobStatus(context.Background(), p.jobID, finalStatus)
	_ = p.store.UpdateJobTimes(context.Background(), p.jobID, false) // Sets ended_at

	// Perform one last metrics flush
	p.saveMetrics(time.Now().Add(-1 * time.Second)) // dummy start time to force save final state

	return nil
}

func (p *Pipeline) startProgressTracker() {
	ticker := time.NewTicker(100 * time.Millisecond)
	startTime := time.Now()

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-p.ctx.Done():
				p.saveMetrics(startTime)
				return
			case <-ticker.C:
				p.saveMetrics(startTime)
				// If pipeline completed or failed, we exit the progress loop
				stage := p.getCurrentStage()
				if stage == "finished" || stage == "cancelled" || stage == "failed" {
					return
				}
			}
		}
	}()
}

func (p *Pipeline) saveMetrics(startTime time.Time) {
	elapsed := time.Since(startTime).Seconds()
	if elapsed <= 0 {
		elapsed = 0.1
	}

	ingested := atomic.LoadInt64(&p.ingestedCount)
	validated := atomic.LoadInt64(&p.validatedCount)
	transformed := atomic.LoadInt64(&p.transformedCount)
	aggregated := atomic.LoadInt64(&p.aggregatedCount)
	exported := atomic.LoadInt64(&p.exportedCount)
	failed := atomic.LoadInt64(&p.failedCount)

	rate := float64(ingested) / elapsed

	m := &models.JobMetrics{
		JobID:              p.jobID,
		RecordsIngested:    ingested,
		RecordsValidated:   validated,
		RecordsTransformed: transformed,
		RecordsAggregated:  aggregated,
		RecordsExported:    exported,
		RecordsFailed:      failed,
		ProcessingRateEPS:  rate,
		CurrentStage:       p.getCurrentStage(),
	}

	// Use background context for updating metrics in store to guarantee write
	_ = p.store.UpdateJobMetrics(context.Background(), m)
}

func (p *Pipeline) startErrorCollector(errCh <-chan *models.JobError, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for jobErr := range errCh {
			_ = p.store.AddJobError(context.Background(), jobErr)
		}
	}()
}
