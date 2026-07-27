package presence

import (
	"context"
	"log"
	"time"

	"collabflow/internal/messaging"
	"collabflow/internal/redis"
)

// CleanupWorker runs periodic scans to evict inactive users from Redis presence and publish user_left events.
type CleanupWorker struct {
	store     *redis.PresenceStore
	publisher *redis.Publisher
	interval  time.Duration
	timeout   time.Duration
}

// NewCleanupWorker initializes a CleanupWorker. Default interval: 30s, inactivity threshold: 30s.
func NewCleanupWorker(store *redis.PresenceStore, publisher *redis.Publisher) *CleanupWorker {
	return &CleanupWorker{
		store:     store,
		publisher: publisher,
		interval:  30 * time.Second,
		timeout:   30 * time.Second,
	}
}

// Start runs the periodic cleanup loop until ctx is cancelled.
func (c *CleanupWorker) Start(ctx context.Context) {
	if c.store == nil {
		log.Printf("[CLEANUP-WORKER] Presence store disabled. Skipping cleanup worker.")
		return
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	log.Printf("[CLEANUP-WORKER] Background presence cleanup worker started (Interval: %v, Timeout: %v)", c.interval, c.timeout)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[CLEANUP-WORKER] Stopping cleanup worker...")
			return
		case <-ticker.C:
			c.RunCleanup(ctx)
		}
	}
}

// RunCleanup performs a single cleanup pass across all active document presence sets.
func (c *CleanupWorker) RunCleanup(ctx context.Context) {
	if c.store == nil {
		return
	}

	docIDs, err := c.store.GetActiveDocumentIDs(ctx)
	if err != nil {
		log.Printf("[CLEANUP-WORKER] Error scanning active document IDs: %v", err)
		return
	}

	cutoff := time.Now().Unix() - int64(c.timeout.Seconds())

	for _, docID := range docIDs {
		evictedUsers, err := c.store.CleanupOfflineUsers(ctx, docID, cutoff)
		if err != nil {
			log.Printf("[CLEANUP-WORKER] Error cleaning up offline users for doc %s: %v", docID, err)
			continue
		}

		for _, userID := range evictedUsers {
			log.Printf("[CLEANUP-WORKER] Evicted inactive user %s from document %s (Inactivity > %v)", userID, docID, c.timeout)

			// Publish user_left event so all WS servers update clients
			if c.publisher != nil {
				event := messaging.Event{
					Type:       messaging.EventTypeUserLeft,
					DocumentID: docID,
					UserID:     userID,
					ServerID:   "CLEANUP-WORKER",
					Timestamp:  time.Now().Unix(),
				}
				if err := c.publisher.Publish(ctx, docID, event); err != nil {
					log.Printf("[CLEANUP-WORKER] Failed to publish eviction event for user %s: %v", userID, err)
				}
			}
		}
	}
}
