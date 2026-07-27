package presence

import (
	"context"
	"log"
	"time"

	"collabflow/internal/redis"
)

// HeartbeatHandler processes incoming client connection heartbeats.
type HeartbeatHandler struct {
	store *redis.PresenceStore
}

// NewHeartbeatHandler creates a new HeartbeatHandler.
func NewHeartbeatHandler(store *redis.PresenceStore) *HeartbeatHandler {
	return &HeartbeatHandler{store: store}
}

// ProcessHeartbeat updates the user's last-seen timestamp score in Redis.
func (h *HeartbeatHandler) ProcessHeartbeat(ctx context.Context, docID, userID string) error {
	if h.store == nil {
		return nil
	}
	now := time.Now().Unix()
	if err := h.store.AddOrUpdateUser(ctx, docID, userID, now); err != nil {
		log.Printf("[HEARTBEAT] Failed to update heartbeat for user %s in doc %s: %v", userID, docID, err)
		return err
	}
	return nil
}
