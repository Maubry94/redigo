package types

type CommandName string

const (
	SET    CommandName = "SET"
	DELETE CommandName = "DELETE"
	EXPIRE CommandName = "EXPIRE"
)

type CommandValue struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type Command struct {
	Name      CommandName  `json:"name"`
	Key       string       `json:"key"`
	Value     CommandValue `json:"value"`
	Ttl       *int64       `json:"ttl,omitempty"`
	Timestamp int64        `json:"timestamp"`
}
