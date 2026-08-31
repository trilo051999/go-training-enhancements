package models

type Record struct {
	ID       string                 `json:"id"`
	Source   string                 `json:"source"`
	Sequence int64                  `json:"sequence"`
	Payload  map[string]interface{} `json:"payload"`
}
