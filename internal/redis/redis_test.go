package redis

import (
	"context"
	"testing"
	"time"

	"collabflow/internal/messaging"
	"github.com/alicebob/miniredis/v2"
)

func TestPublisherSubscriberIntegration(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client, err := NewClient(mr.Addr())
	if err != nil {
		t.Fatalf("Failed to connect to miniredis: %v", err)
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
	time.Sleep(100 * time.Millisecond)

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
