package pipeline

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

func (p *Pipeline) startValidationStage(recordsCh <-chan models.Record, validatedCh chan<- models.Record, errCh chan<- *models.JobError, wg *sync.WaitGroup) {
	numWorkers := p.spec.Concurrency.ValidationWorkers
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case record, ok := <-recordsCh:
					if !ok {
						return
					}

					if err := validateRecord(record, p.spec.Validation.Rules); err != nil {
						rawRecordBytes, _ := json.Marshal(record.Payload)
						errCh <- &models.JobError{
							JobID:        p.jobID,
							RecordID:     record.ID,
							Stage:        "validate",
							ErrorMessage: err.Error(),
							RawRecord:    string(rawRecordBytes),
							CreatedAt:    time.Now(),
						}
						atomic.AddInt64(&p.failedCount, 1)
					} else {
						select {
						case <-p.ctx.Done():
							return
						case validatedCh <- record:
							atomic.AddInt64(&p.validatedCount, 1)
						}
					}
				}
			}
		}()
	}
}

func validateRecord(record models.Record, rules []models.ValidationRule) error {
	for _, rule := range rules {
		val, exists := record.Payload[rule.Field]
		if !exists || val == nil {
			if rule.Rule == "required" {
				return fmt.Errorf("field %q is required but missing", rule.Field)
			}
			continue
		}

		switch rule.Rule {
		case "required":
			if val == "" {
				return fmt.Errorf("field %q is empty", rule.Field)
			}
		case "min":
			numVal, isNum := convertToFloat(val)
			if !isNum {
				return fmt.Errorf("field %q is not numeric for min check", rule.Field)
			}
			minVal, isMinNum := convertToFloat(rule.Value)
			if !isMinNum {
				return fmt.Errorf("min threshold for field %q is not numeric", rule.Field)
			}
			if numVal < minVal {
				return fmt.Errorf("field %q value %v is less than min %v", rule.Field, val, rule.Value)
			}
		case "max":
			numVal, isNum := convertToFloat(val)
			if !isNum {
				return fmt.Errorf("field %q is not numeric for max check", rule.Field)
			}
			maxVal, isMaxNum := convertToFloat(rule.Value)
			if !isMaxNum {
				return fmt.Errorf("max threshold for field %q is not numeric", rule.Field)
			}
			if numVal > maxVal {
				return fmt.Errorf("field %q value %v is greater than max %v", rule.Field, val, rule.Value)
			}
		case "regex":
			pattern, ok := rule.Value.(string)
			if !ok {
				return fmt.Errorf("regex pattern for field %q must be a string", rule.Field)
			}
			matched, err := regexp.MatchString(pattern, fmt.Sprintf("%v", val))
			if err != nil {
				return fmt.Errorf("invalid regex pattern: %w", err)
			}
			if !matched {
				return fmt.Errorf("field %q value %v does not match regex %q", rule.Field, val, pattern)
			}
		case "type":
			expectedType, ok := rule.Value.(string)
			if !ok {
				return fmt.Errorf("type rule value for field %q must be a string", rule.Field)
			}
			if err := checkType(val, expectedType); err != nil {
				return fmt.Errorf("field %q: %w", rule.Field, err)
			}
		}
	}
	return nil
}

func convertToFloat(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		var f float64
		_, err := fmt.Sscanf(v, "%f", &f)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func checkType(val interface{}, expectedType string) error {
	switch expectedType {
	case "integer":
		switch val.(type) {
		case int, int32, int64:
			return nil
		case float64:
			f := val.(float64)
			if f == float64(int64(f)) {
				return nil
			}
			return fmt.Errorf("value %v is float, not integer", val)
		case string:
			var i int64
			_, err := fmt.Sscanf(val.(string), "%d", &i)
			if err != nil {
				return fmt.Errorf("value %q cannot be parsed as integer", val)
			}
			return nil
		}
	case "float", "numeric":
		_, ok := convertToFloat(val)
		if ok {
			return nil
		}
		return fmt.Errorf("value %v is not numeric", val)
	case "boolean":
		switch v := val.(type) {
		case bool:
			return nil
		case string:
			if v == "true" || v == "1" {
				return nil
			}
			if v == "false" || v == "0" {
				return nil
			}
		}
		return fmt.Errorf("value %v is not boolean", val)
	case "string":
		_, ok := val.(string)
		if ok {
			return nil
		}
		return fmt.Errorf("value %v is not a string", val)
	}
	return fmt.Errorf("unknown type check %q", expectedType)
}
