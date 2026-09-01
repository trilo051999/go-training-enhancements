package pipeline

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

func (p *Pipeline) startTransformationStage(validatedCh <-chan models.Record, transformedCh chan<- models.Record, errCh chan<- *models.JobError, wg *sync.WaitGroup) {
	numWorkers := p.spec.Concurrency.TransformWorkers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case record, ok := <-validatedCh:
					if !ok {
						return
					}

					txRecord, err := transformRecord(record, p.spec.Transformations)
					if err != nil {
						rawRecordBytes, _ := json.Marshal(record.Payload)
						errCh <- &models.JobError{
							JobID:        p.jobID,
							RecordID:     record.ID,
							Stage:        "transform",
							ErrorMessage: err.Error(),
							RawRecord:    string(rawRecordBytes),
							CreatedAt:    time.Now(),
						}
						atomic.AddInt64(&p.failedCount, 1)
					} else {
						select {
						case <-p.ctx.Done():
							return
						case transformedCh <- txRecord:
							atomic.AddInt64(&p.transformedCount, 1)
						}
					}
				}
			}
		}()
	}
}

func transformRecord(record models.Record, transforms []models.TransformConfig) (models.Record, error) {
	// Create a shallow copy of the payload to isolate changes
	newPayload := make(map[string]interface{})
	for k, v := range record.Payload {
		newPayload[k] = v
	}

	for _, tx := range transforms {
		switch tx.Type {
		case "rename":
			from := tx.Params["from"]
			to := tx.Params["to"]
			if from == "" || to == "" {
				return record, fmt.Errorf("rename transform requires 'from' and 'to' params")
			}
			if val, exists := newPayload[from]; exists {
				newPayload[to] = val
				delete(newPayload, from)
			}
		case "cast":
			field := tx.Params["field"]
			toType := tx.Params["to_type"]
			if field == "" || toType == "" {
				return record, fmt.Errorf("cast transform requires 'field' and 'to_type' params")
			}
			val, exists := newPayload[field]
			if !exists || val == nil {
				continue
			}
			castVal, err := castValue(val, toType)
			if err != nil {
				return record, fmt.Errorf("failed to cast field %q to %q: %w", field, toType, err)
			}
			newPayload[field] = castVal
		case "default":
			field := tx.Params["field"]
			defaultVal := tx.Params["value"]
			if field == "" {
				return record, fmt.Errorf("default transform requires 'field' param")
			}
			if val, exists := newPayload[field]; !exists || val == nil || val == "" {
				newPayload[field] = defaultVal
			}
		case "math":
			field := tx.Params["field"]
			toField := tx.Params["to_field"]
			multiplierStr := tx.Params["multiplier"]
			if field == "" || toField == "" || multiplierStr == "" {
				return record, fmt.Errorf("math transform requires 'field', 'to_field', and 'multiplier' params")
			}
			val, exists := newPayload[field]
			if !exists || val == nil {
				continue
			}
			numVal, ok := convertToFloat(val)
			if !ok {
				return record, fmt.Errorf("field %q is not numeric for math operation", field)
			}
			var multiplier float64
			if _, err := fmt.Sscanf(multiplierStr, "%f", &multiplier); err != nil {
				return record, fmt.Errorf("invalid multiplier %q: %w", multiplierStr, err)
			}
			newPayload[toField] = numVal * multiplier
		}
	}

	record.Payload = newPayload
	return record, nil
}

func castValue(val interface{}, toType string) (interface{}, error) {
	switch toType {
	case "integer":
		switch v := val.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err != nil {
				var f float64
				if _, errF := fmt.Sscanf(v, "%f", &f); errF == nil {
					return int64(f), nil
				}
				return nil, err
			}
			return i, nil
		}
	case "float", "double", "numeric":
		f, ok := convertToFloat(val)
		if !ok {
			return nil, fmt.Errorf("cannot convert %v to float", val)
		}
		return f, nil
	case "string":
		return fmt.Sprintf("%v", val), nil
	case "boolean":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			if v == "true" || v == "1" {
				return true, nil
			}
			if v == "false" || v == "0" {
				return false, nil
			}
		case int, int64, float64:
			f, _ := convertToFloat(v)
			return f != 0, nil
		}
		return nil, fmt.Errorf("cannot convert %v to boolean", val)
	}
	return nil, fmt.Errorf("unknown cast type: %s", toType)
}
