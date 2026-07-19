package redis

import (
	"context"
	"testing"
	"time"

	"collabflow/internal/messaging"
)

func TestPublisherSubscriberIntegration(t *testing.T) {
	// Attempt to connect to local Redis instance (e.g. docker or local redis)
	client, err := NewClient("localhost:6379")
	if err != nil {
		t.Skipf("Skipping live Redis test (Redis not available at localhost:6379): %v", err)
	}
	defer client.Close()

	pub := NewPublisher(client)
	sub := NewSubscriber(client, "TEST-SERVER")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	received := make(chan messaging.Event, 1)

	go func() {
		_ = sub.StartListening(ctx, func(docID string, event messaging.Event) {
			if docID == "test_doc_999" {
				received <- event
			}
		})
	}()

	// Give subscriber time to establish PSubscribe connection
	time.Sleep(200 * time.Millisecond)

	testEvent := messaging.Event{
		Type:       "insert",
		DocumentID: "test_doc_999",
		UserID:     "user_test",
		Content:    "Hello PubSub",
		ServerID:   "TEST-SERVER",
	}

	if err := pub.Publish(ctx, "test_doc_999", testEvent); err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	select {
	case evt := <-received:
		if evt.Content != "Hello PubSub" {
			t.Errorf("Expected content 'Hello PubSub', got '%s'", evt.Content)
		}
		if evt.UserID != "user_test" {
			t.Errorf("Expected UserID 'user_test', got '%s'", evt.UserID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for Redis pubsub event")
	}
}
