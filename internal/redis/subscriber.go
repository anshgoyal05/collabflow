package redis

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"collabflow/internal/messaging"
	"github.com/redis/go-redis/v9"
)

// EventHandler defines a function signature for processing received events.
type EventHandler func(documentID string, event messaging.Event)

// Subscriber listens to Redis Pub/Sub channels for document updates.
type Subscriber struct {
	rdb      *redis.Client
	serverID string
}

// NewSubscriber constructs a new Subscriber.
func NewSubscriber(rdb *redis.Client, serverID string) *Subscriber {
	return &Subscriber{
		rdb:      rdb,
		serverID: serverID,
	}
}

// StartListening subscribes to pattern `document:*` and invokes handler for every event.
// This call blocks until ctx is cancelled or sub is closed.
func (s *Subscriber) StartListening(ctx context.Context, handler EventHandler) error {
	pubsub := s.rdb.PSubscribe(ctx, "document:*")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}

			// Extract document ID from channel name (e.g., "document:doc_123" -> "doc_123")
			docID := strings.TrimPrefix(msg.Channel, "document:")

			var event messaging.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Printf("[%s] Error unmarshaling redis event payload: %v", s.serverID, err)
				continue
			}

			// Ensure document ID is populated on event
			if event.DocumentID == "" {
				event.DocumentID = docID
			}

			handler(docID, event)
		}
	}
}
