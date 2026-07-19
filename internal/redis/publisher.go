package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"collabflow/internal/messaging"
	"github.com/redis/go-redis/v9"
)

// Publisher handles publishing events to Redis channels.
type Publisher struct {
	rdb *redis.Client
}

// NewPublisher constructs a new Publisher.
func NewPublisher(rdb *redis.Client) *Publisher {
	return &Publisher{rdb: rdb}
}

// Publish serializes and publishes an event to the Redis channel `document:<documentID>`.
func (p *Publisher) Publish(ctx context.Context, documentID string, event messaging.Event) error {
	channel := GetDocumentChannel(documentID)
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event for publishing: %w", err)
	}

	if err := p.rdb.Publish(ctx, channel, data).Err(); err != nil {
		return fmt.Errorf("failed to publish message to redis channel %s: %w", channel, err)
	}
	return nil
}

// GetDocumentChannel constructs a standardized Redis channel name for a document ID.
func GetDocumentChannel(documentID string) string {
	return fmt.Sprintf("document:%s", documentID)
}
