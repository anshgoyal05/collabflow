package messaging

import (
	"encoding/json"
	"fmt"
)

// Standard Event Type Constants
const (
	EventTypeJoinDocument   = "join_document"
	EventTypePresenceUpdate = "presence_update"
	EventTypeUserJoined     = "user_joined"
	EventTypeUserLeft       = "user_left"
	EventTypeHeartbeat      = "heartbeat"
	EventTypeCursorMove     = "cursor_move"
	EventTypeCursorUpdate   = "cursor_update"
	EventTypeTyping         = "typing"
	EventTypeTypingStart    = "typing_start"
	EventTypeUserTyping     = "user_typing"
	EventTypeInsert         = "insert"
	EventTypeDelete         = "delete"
)

// Position holds position data, supporting both integer index (edits) and coordinate objects (cursors).
type Position struct {
	Index  int `json:"index,omitempty"`
	Line   int `json:"line,omitempty"`
	Column int `json:"column,omitempty"`
	X      int `json:"x,omitempty"`
	Y      int `json:"y,omitempty"`

	Raw json.RawMessage `json:"-"`
}

// UnmarshalJSON implements custom deserialization for Position.
func (p *Position) UnmarshalJSON(data []byte) error {
	p.Raw = append([]byte(nil), data...)
	var intVal int
	if err := json.Unmarshal(data, &intVal); err == nil {
		p.Index = intVal
		return nil
	}
	type Alias Position
	var alias Alias
	if err := json.Unmarshal(data, &alias); err == nil {
		*p = Position(alias)
		p.Raw = append([]byte(nil), data...)
		return nil
	}
	return nil
}

// MarshalJSON implements custom serialization for Position.
func (p Position) MarshalJSON() ([]byte, error) {
	if len(p.Raw) > 0 {
		return p.Raw, nil
	}
	if p.Line != 0 || p.Column != 0 || p.X != 0 || p.Y != 0 {
		type Alias Position
		return json.Marshal((Alias)(p))
	}
	return json.Marshal(p.Index)
}

// String returns raw JSON string representation of Position.
func (p Position) String() string {
	if len(p.Raw) > 0 {
		return string(p.Raw)
	}
	b, err := p.MarshalJSON()
	if err != nil {
		return fmt.Sprintf("index:%d line:%d col:%d x:%d y:%d", p.Index, p.Line, p.Column, p.X, p.Y)
	}
	return string(b)
}

// Event represents a messaging payload exchanged across WebSockets and Redis Pub/Sub.
type Event struct {
	Type        string          `json:"type"`                 // e.g. "join_document", "presence_update", "cursor_move", etc.
	DocumentID  string          `json:"documentId,omitempty"` // Target document ID
	UserID      string          `json:"userId,omitempty"`     // Unique ID of the sending user
	Position    *Position       `json:"position,omitempty"`   // Edit index or cursor position object
	Content     string          `json:"content,omitempty"`    // Raw string content
	Value       string          `json:"value,omitempty"`      // Alternate payload field for value
	ServerID    string          `json:"serverId,omitempty"`   // ID of the originating WebSocket server
	Timestamp   int64           `json:"timestamp,omitempty"`  // Unix timestamp
	OnlineUsers []string        `json:"onlineUsers,omitempty"`// Active online user IDs
	Status      *bool           `json:"status,omitempty"`     // Typing status flag
	Payload     json.RawMessage `json:"payload,omitempty"`    // Raw JSON payload for CRDT or extra metadata
}

// GetContent returns Content or Value if Content is empty.
func (e Event) GetContent() string {
	if e.Content != "" {
		return e.Content
	}
	return e.Value
}

