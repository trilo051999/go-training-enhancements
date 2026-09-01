package pipeline

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

func (p *Pipeline) startIngestionStage(recordsCh chan<- models.Record, errCh chan<- *models.JobError, wg *sync.WaitGroup) {
	for _, source := range p.spec.Sources {
		wg.Add(1)
		go func(src models.SourceConfig) {
			defer wg.Done()

			reader, err := openSource(p.ctx, src.Path)
			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				errCh <- &models.JobError{
					JobID:        p.jobID,
					Stage:        "ingest",
					ErrorMessage: fmt.Sprintf("failed to open source %s (%s): %v", src.ID, src.Path, err),
					CreatedAt:    time.Now(),
				}
				atomic.AddInt64(&p.failedCount, 1)
				return
			}
			defer reader.Close()

			switch src.Type {
			case "csv":
				err = p.parseCSV(reader, src.ID, recordsCh, errCh)
			case "json", "api":
				err = p.parseJSON(reader, src.ID, recordsCh, errCh)
			default:
				err = fmt.Errorf("unsupported source type %q", src.Type)
			}

			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				errCh <- &models.JobError{
					JobID:        p.jobID,
					Stage:        "ingest",
					ErrorMessage: fmt.Sprintf("error parsing source %s: %v", src.ID, err),
					CreatedAt:    time.Now(),
				}
				atomic.AddInt64(&p.failedCount, 1)
			}
		}(source)
	}
}

func openSource(ctx context.Context, path string) (io.ReadCloser, error) {
	// If path is a web URL, download it using context to support cancellations
	if len(path) > 4 && (path[:7] == "http://" || path[:8] == "https://") {
		req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("http error: %s", resp.Status)
		}
		return resp.Body, nil
	}
	return os.Open(path)
}

func (p *Pipeline) parseCSV(reader io.Reader, sourceID string, recordsCh chan<- models.Record, errCh chan<- *models.JobError) error {
	csvReader := csv.NewReader(reader)
	// Check for a custom delimiter (default is comma)
	if delimiter, exists := p.spec.Sources[0].Params["delimiter"]; exists && len(delimiter) > 0 {
		csvReader.Comma = rune(delimiter[0])
	}

	headers, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read headers: %w", err)
	}

	var seq int64 = 1
	for {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errCh <- &models.JobError{
				JobID:        p.jobID,
				Stage:        "ingest",
				ErrorMessage: fmt.Sprintf("failed to read row at sequence %d: %v", seq, err),
				CreatedAt:    time.Now(),
			}
			atomic.AddInt64(&p.failedCount, 1)
			seq++
			continue
		}

		payload := make(map[string]interface{})
		for i, val := range row {
			if i < len(headers) {
				payload[headers[i]] = val
			}
		}

		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case recordsCh <- models.Record{
			ID:       fmt.Sprintf("%s-%d", sourceID, seq),
			Source:   sourceID,
			Sequence: seq,
			Payload:  payload,
		}:
			atomic.AddInt64(&p.ingestedCount, 1)
		}
		seq++
	}
	return nil
}

func (p *Pipeline) parseJSON(reader io.Reader, sourceID string, recordsCh chan<- models.Record, errCh chan<- *models.JobError) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return fmt.Errorf("empty JSON payload")
	}

	if trimmed[0] == '[' {
		var list []map[string]interface{}
		if err := json.Unmarshal(trimmed, &list); err != nil {
			return err
		}

		for i, item := range list {
			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			default:
			}

			select {
			case <-p.ctx.Done():
				return p.ctx.Err()
			case recordsCh <- models.Record{
				ID:       fmt.Sprintf("%s-%d", sourceID, i+1),
				Source:   sourceID,
				Sequence: int64(i + 1),
				Payload:  item,
			}:
				atomic.AddInt64(&p.ingestedCount, 1)
			}
		}
	} else if trimmed[0] == '{' {
		var item map[string]interface{}
		if err := json.Unmarshal(trimmed, &item); err != nil {
			return err
		}

		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		case recordsCh <- models.Record{
			ID:       fmt.Sprintf("%s-1", sourceID),
			Source:   sourceID,
			Sequence: 1,
			Payload:  item,
		}:
			atomic.AddInt64(&p.ingestedCount, 1)
		}
	} else {
		return fmt.Errorf("invalid JSON start character: %c", trimmed[0])
	}

	return nil
}
