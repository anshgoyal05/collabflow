package models

import "encoding/json"

// Message represents a basic communication packet sent over WebSockets.
type Message struct {
	Type       string          `json:"type"`                 // e.g. "insert", "delete", "join", "leave"
	UserID     string          `json:"userId"`               // Unique ID of the sending user
	DocumentID string          `json:"documentId"`           // Document room ID
	Content    string          `json:"content,omitempty"`    // Raw string content (for now)
	Payload    json.RawMessage `json:"payload,omitempty"`    // Future CRDT operations
}
