package typing

import (
	"context"
	"fmt"
	"log"
	"time"

	"collabflow/internal/messaging"
	"collabflow/internal/redis"
)

// Indicator manages typing indicator state with automatic TTL expiration.
type Indicator struct {
	store     *redis.PresenceStore
	publisher *redis.Publisher
	ttl       time.Duration
}

// NewIndicator creates a new TypingIndicator with default 3-second TTL.
func NewIndicator(store *redis.PresenceStore, publisher *redis.Publisher) *Indicator {
	return &Indicator{
		store:     store,
		publisher: publisher,
		ttl:       3 * time.Second,
	}
}

// SetTyping updates Redis typing status with 3s TTL and broadcasts a user_typing event.
func (i *Indicator) SetTyping(ctx context.Context, docID, userID, serverID string, status bool) error {
	if i.store != nil {
		if status {
			if err := i.store.SetTyping(ctx, docID, userID, i.ttl); err != nil {
				log.Printf("[TYPING] Error setting typing key for user %s in doc %s: %v", userID, docID, err)
			}
		} else {
			if err := i.store.RemoveTyping(ctx, docID, userID); err != nil {
				log.Printf("[TYPING] Error removing typing key for user %s in doc %s: %v", userID, docID, err)
			}
		}
	}

	event := messaging.Event{
		Type:       messaging.EventTypeUserTyping,
		DocumentID: docID,
		UserID:     userID,
		Status:     &status,
		ServerID:   serverID,
		Timestamp:  time.Now().Unix(),
	}

	if i.publisher != nil {
		if err := i.publisher.Publish(ctx, docID, event); err != nil {
			return fmt.Errorf("failed to publish user_typing event: %w", err)
		}
	}

	return nil
}

// IsTyping checks if typing key exists in Redis.
func (i *Indicator) IsTyping(ctx context.Context, docID, userID string) (bool, error) {
	if i.store == nil {
		return false, nil
	}
	return i.store.IsTyping(ctx, docID, userID)
}
