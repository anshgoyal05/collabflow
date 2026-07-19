package messaging

import "encoding/json"

// Event represents a messaging payload exchanged across WebSockets and Redis Pub/Sub.
type Event struct {
	Type       string          `json:"type"`                 // e.g. "insert", "delete", "join", "leave"
	DocumentID string          `json:"documentId,omitempty"` // Target document ID
	UserID     string          `json:"userId,omitempty"`     // Unique ID of the sending user
	Position   int             `json:"position,omitempty"`   // Edit position (optional)
	Content    string          `json:"content,omitempty"`    // Raw string content
	Value      string          `json:"value,omitempty"`      // Alternate payload field for value
	ServerID   string          `json:"serverId,omitempty"`   // ID of the originating WebSocket server
	Timestamp  int64           `json:"timestamp,omitempty"`  // Unix timestamp (milliseconds)
	Payload    json.RawMessage `json:"payload,omitempty"`    // Raw JSON payload for CRDT or future expansions
}

// GetContent returns Content or Value if Content is empty.
func (e Event) GetContent() string {
	if e.Content != "" {
		return e.Content
	}
	return e.Value
}
