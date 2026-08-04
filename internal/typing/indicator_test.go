package typing_test

import (
	"context"
	"testing"
	"time"

	"collabflow/internal/redis"
	"collabflow/internal/typing"

	"github.com/alicebob/miniredis/v2"
)

func TestTypingIndicator(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb, err := redis.NewClient(mr.Addr())
	if err != nil {
		t.Fatalf("Failed to connect to miniredis: %v", err)
	}
	defer rdb.Close()

	store := redis.NewPresenceStore(rdb)
	pub := redis.NewPublisher(rdb)
	indicator := typing.NewIndicator(store, pub)

	ctx := context.Background()
	docID := "doc_typing_1"
	userID := "user_t1"

	// Initial state
	isTyping, err := indicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing: %v", err)
	}
	if isTyping {
		t.Errorf("Expected user not to be typing initially")
	}

	// Start typing
	if err := indicator.SetTyping(ctx, docID, userID, "SERVER-1", true); err != nil {
		t.Fatalf("Failed to set typing: %v", err)
	}

	isTyping, err = indicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing: %v", err)
	}
	if !isTyping {
		t.Errorf("Expected user to be typing")
	}

	// Stop typing
	if err := indicator.SetTyping(ctx, docID, userID, "SERVER-1", false); err != nil {
		t.Fatalf("Failed to set typing false: %v", err)
	}

	isTyping, err = indicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing: %v", err)
	}
	if isTyping {
		t.Errorf("Expected user to stop typing immediately")
	}

	// Test TTL expiration with miniredis FastForward
	_ = indicator.SetTyping(ctx, docID, userID, "SERVER-1", true)
	mr.FastForward(4 * time.Second)

	isTyping, err = indicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing after TTL: %v", err)
	}
	if isTyping {
		t.Errorf("Expected typing key to expire after 3s TTL")
	}
}
