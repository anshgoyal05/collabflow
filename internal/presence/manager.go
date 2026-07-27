package presence

import (
	"context"
	"fmt"
	"log"
	"time"

	"collabflow/internal/messaging"
	"collabflow/internal/redis"
)

// Manager coordinates presence operations between memory, Redis, and Pub/Sub.
type Manager struct {
	store     *redis.PresenceStore
	publisher *redis.Publisher
}

// NewManager constructs a new Presence Manager.
func NewManager(store *redis.PresenceStore, publisher *redis.Publisher) *Manager {
	return &Manager{
		store:     store,
		publisher: publisher,
	}
}

// RegisterUser registers a user's presence in Redis and broadcasts a user_joined event.
func (m *Manager) RegisterUser(ctx context.Context, docID, userID, serverID string) ([]string, error) {
	now := time.Now().Unix()
	if m.store != nil {
		if err := m.store.AddOrUpdateUser(ctx, docID, userID, now); err != nil {
			log.Printf("[PRESENCE] Error adding user %s to doc %s: %v", userID, docID, err)
		}
	}

	onlineUsers, err := m.GetOnlineUsers(ctx, docID)
	if err != nil {
		onlineUsers = []string{userID}
	}

	// Broadcast user_joined event across Redis Pub/Sub
	event := messaging.Event{
		Type:        messaging.EventTypeUserJoined,
		DocumentID:  docID,
		UserID:      userID,
		ServerID:    serverID,
		Timestamp:   now,
		OnlineUsers: onlineUsers,
	}

	if m.publisher != nil {
		if err := m.publisher.Publish(ctx, docID, event); err != nil {
			log.Printf("[PRESENCE] Error publishing user_joined event: %v", err)
		}
	}

	return onlineUsers, nil
}

// UnregisterUser removes a user from presence and broadcasts a user_left event.
func (m *Manager) UnregisterUser(ctx context.Context, docID, userID, serverID string) error {
	if m.store != nil {
		if err := m.store.RemoveUser(ctx, docID, userID); err != nil {
			log.Printf("[PRESENCE] Error removing user %s from doc %s: %v", userID, docID, err)
		}
	}

	now := time.Now().Unix()
	event := messaging.Event{
		Type:       messaging.EventTypeUserLeft,
		DocumentID: docID,
		UserID:     userID,
		ServerID:   serverID,
		Timestamp:  now,
	}

	if m.publisher != nil {
		if err := m.publisher.Publish(ctx, docID, event); err != nil {
			return fmt.Errorf("failed to publish user_left event: %w", err)
		}
	}

	return nil
}

// GetOnlineUsers fetches active users (heartbeat within 30 seconds).
func (m *Manager) GetOnlineUsers(ctx context.Context, docID string) ([]string, error) {
	if m.store == nil {
		return []string{}, nil
	}
	cutoff := time.Now().Unix() - 30
	return m.store.GetOnlineUsers(ctx, docID, cutoff)
}
