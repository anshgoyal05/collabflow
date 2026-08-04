package cursor_test

import (
	"context"
	"testing"

	"collabflow/internal/cursor"
	"collabflow/internal/messaging"
	"collabflow/internal/redis"

	"github.com/alicebob/miniredis/v2"
)

func TestCursorTracker(t *testing.T) {
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
	tracker := cursor.NewTracker(store, pub)

	ctx := context.Background()
	docID := "doc_cursor_1"
	userID := "user_c1"

	pos := &messaging.Position{Line: 12, Column: 34}
	if err := tracker.UpdateCursor(ctx, docID, userID, "SERVER-1", pos); err != nil {
		t.Fatalf("Failed to update cursor: %v", err)
	}

	cursors, err := tracker.GetCursors(ctx, docID)
	if err != nil {
		t.Fatalf("Failed to get cursors: %v", err)
	}

	if cursors[userID] == "" {
		t.Errorf("Expected position for user %s, got empty", userID)
	}
}
