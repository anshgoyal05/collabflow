package cursor

import (
	"context"
	"fmt"
	"log"
	"time"

	"collabflow/internal/messaging"
	"collabflow/internal/redis"
)

// Tracker manages real-time cursor position tracking in Redis.
type Tracker struct {
	store     *redis.PresenceStore
	publisher *redis.Publisher
}

// NewTracker constructs a new cursor Tracker.
func NewTracker(store *redis.PresenceStore, publisher *redis.Publisher) *Tracker {
	return &Tracker{
		store:     store,
		publisher: publisher,
	}
}

// UpdateCursor persists a user's cursor position in Redis Hash cursor:{docID} and broadcasts the update.
func (t *Tracker) UpdateCursor(ctx context.Context, docID, userID, serverID string, position *messaging.Position) error {
	if position == nil {
		return nil
	}

	posStr := position.String()

	if t.store != nil {
		if err := t.store.SetCursor(ctx, docID, userID, posStr); err != nil {
			log.Printf("[CURSOR] Error storing cursor for user %s in doc %s: %v", userID, docID, err)
		}
	}

	event := messaging.Event{
		Type:       messaging.EventTypeCursorMove,
		DocumentID: docID,
		UserID:     userID,
		Position:   position,
		ServerID:   serverID,
		Timestamp:  time.Now().Unix(),
	}

	if t.publisher != nil {
		if err := t.publisher.Publish(ctx, docID, event); err != nil {
			return fmt.Errorf("failed to publish cursor update: %w", err)
		}
	}

	return nil
}

// GetCursors retrieves all active cursor positions for a document.
func (t *Tracker) GetCursors(ctx context.Context, docID string) (map[string]string, error) {
	if t.store == nil {
		return map[string]string{}, nil
	}
	return t.store.GetCursors(ctx, docID)
}
