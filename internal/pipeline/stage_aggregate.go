package pipeline

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/models"
)

type GroupState struct {
	GroupMap map[string]interface{}
	Count    int64
	Metrics  map[string]*MetricState
}

type MetricState struct {
	Sum      float64
	Count    int64
	Min      float64
	Max      float64
	HasValue bool
}

func (p *Pipeline) startAggregationStage(transformedCh <-chan models.Record, exportCh chan<- map[string]interface{}, errCh chan<- *models.JobError) {
	go func() {
		defer close(exportCh)

		if p.spec.Aggregation == nil {
			// Without aggregation config, records pass directly to exports as-is
			for {
				select {
				case <-p.ctx.Done():
					return
				case record, ok := <-transformedCh:
					if !ok {
						return
					}
					select {
					case <-p.ctx.Done():
						return
					case exportCh <- record.Payload:
						atomic.AddInt64(&p.aggregatedCount, 1)
					}
				}
			}
		}

		groups := make(map[string]*GroupState)

		for {
			select {
			case <-p.ctx.Done():
				return
			case record, ok := <-transformedCh:
				if !ok {
					goto finalize
				}

				key, groupMap := getGroupKey(record, p.spec.Aggregation.GroupBy)
				gState, exists := groups[key]
				if !exists {
					gState = &GroupState{
						GroupMap: groupMap,
						Metrics:  make(map[string]*MetricState),
					}
					groups[key] = gState
				}
				gState.Count++

				for _, metric := range p.spec.Aggregation.Metrics {
					val, ok := record.Payload[metric.Field]
					if !ok || val == nil {
						continue
					}
					num, isNum := convertToFloat(val)
					if !isNum {
						continue
					}

					mState, exists := gState.Metrics[metric.OutputField]
					if !exists {
						mState = &MetricState{
							Min: math.MaxFloat64,
							Max: -math.MaxFloat64,
						}
						gState.Metrics[metric.OutputField] = mState
					}

					mState.Count++
					mState.Sum += num
					if num < mState.Min {
						mState.Min = num
					}
					if num > mState.Max {
						mState.Max = num
					}
					mState.HasValue = true
				}
			}
		}

	finalize:
		// Map group running metrics to output payloads
		for key, gState := range groups {
			select {
			case <-p.ctx.Done():
				return
			default:
			}

			result := make(map[string]interface{})
			for k, v := range gState.GroupMap {
				result[k] = v
			}

			for _, metric := range p.spec.Aggregation.Metrics {
				mState, exists := gState.Metrics[metric.OutputField]
				if !exists || !mState.HasValue {
					result[metric.OutputField] = nil
					continue
				}

				switch metric.Type {
				case "sum":
					result[metric.OutputField] = mState.Sum
				case "count":
					result[metric.OutputField] = mState.Count
				case "avg":
					if mState.Count > 0 {
						result[metric.OutputField] = mState.Sum / float64(mState.Count)
					} else {
						result[metric.OutputField] = 0.0
					}
				case "min":
					result[metric.OutputField] = mState.Min
				case "max":
					result[metric.OutputField] = mState.Max
				}
			}

			// Persist the row state immediately into SQLite results
			err := p.store.SaveJobResult(p.ctx, p.jobID, key, result)
			if err != nil {
				errCh <- &models.JobError{
					JobID:        p.jobID,
					Stage:        "aggregate",
					ErrorMessage: fmt.Sprintf("failed to save aggregated result for key %q: %v", key, err),
					CreatedAt:    time.Now(),
				}
			}

			select {
			case <-p.ctx.Done():
				return
			case exportCh <- result:
				atomic.AddInt64(&p.aggregatedCount, 1)
			}
		}
	}()
}

func getGroupKey(record models.Record, groupBy []string) (string, map[string]interface{}) {
	if len(groupBy) == 0 {
		return "global", map[string]interface{}{"global": "all"}
	}

	groupMap := make(map[string]interface{})
	keyStr := ""
	for i, field := range groupBy {
		val := record.Payload[field]
		if val == nil {
			val = "null"
		}
		groupMap[field] = val
		if i > 0 {
			keyStr += ","
		}
		keyStr += fmt.Sprintf("%s=%v", field, val)
	}
	return keyStr, groupMap
}
