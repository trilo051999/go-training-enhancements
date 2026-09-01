package pipeline

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

func (p *Pipeline) startExportStage(exportCh <-chan map[string]interface{}, errCh chan<- *models.JobError) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		var results []map[string]interface{}

		for {
			select {
			case <-p.ctx.Done():
				return
			case result, ok := <-exportCh:
				if !ok {
					goto finalizeExport
				}

				results = append(results, result)

				// Export row immediately to table if configured
				for _, exp := range p.spec.Exports {
					if exp.Type == "sqlite" {
						tableName := exp.Params["table"]
						if tableName != "" {
							if err := p.store.ExportToTable(p.ctx, tableName, result); err != nil {
								errCh <- &models.JobError{
									JobID:        p.jobID,
									Stage:        "export",
									ErrorMessage: fmt.Sprintf("failed to export row to sqlite table %q: %v", tableName, err),
									CreatedAt:    time.Now(),
								}
								atomic.AddInt64(&p.failedCount, 1)
								continue
							}
						}
					}
				}
				atomic.AddInt64(&p.exportedCount, 1)
			}
		}

	finalizeExport:
		if len(results) == 0 {
			return
		}

		// Perform batch file writes
		for _, exp := range p.spec.Exports {
			switch exp.Type {
			case "json":
				path := exp.Params["path"]
				if path != "" {
					if err := p.exportToJSONFile(path, results); err != nil {
						errCh <- &models.JobError{
							JobID:        p.jobID,
							Stage:        "export",
							ErrorMessage: fmt.Sprintf("failed to export to json file %s: %v", path, err),
							CreatedAt:    time.Now(),
						}
						atomic.AddInt64(&p.failedCount, 1)
					}
				}
			case "csv":
				path := exp.Params["path"]
				if path != "" {
					if err := p.exportToCSVFile(path, results); err != nil {
						errCh <- &models.JobError{
							JobID:        p.jobID,
							Stage:        "export",
							ErrorMessage: fmt.Sprintf("failed to export to csv file %s: %v", path, err),
							CreatedAt:    time.Now(),
						}
						atomic.AddInt64(&p.failedCount, 1)
					}
				}
			}
		}
	}()

	return done
}

func (p *Pipeline) exportToJSONFile(path string, data []map[string]interface{}) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func (p *Pipeline) exportToCSVFile(path string, data []map[string]interface{}) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if len(data) == 0 {
		return nil
	}

	// Dynamic header discovery based on unique keys across rows
	headersMap := make(map[string]bool)
	var headers []string
	for _, row := range data {
		for k := range row {
			if !headersMap[k] {
				headersMap[k] = true
				headers = append(headers, k)
			}
		}
	}

	if err := writer.Write(headers); err != nil {
		return err
	}

	for _, row := range data {
		csvRow := make([]string, len(headers))
		for i, h := range headers {
			val := row[h]
			if val == nil {
				csvRow[i] = ""
			} else {
				csvRow[i] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.Write(csvRow); err != nil {
			return err
		}
	}

	return nil
}
