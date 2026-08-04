package presence_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"collabflow/internal/cursor"
	"collabflow/internal/messaging"
	"collabflow/internal/presence"
	"collabflow/internal/redis"
	"collabflow/internal/typing"
	"collabflow/internal/websocket"

	"github.com/alicebob/miniredis/v2"
	gorilla "github.com/gorilla/websocket"
)

func setupMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.PresenceStore, *redis.Publisher) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	rdb, err := redis.NewClient(mr.Addr())
	if err != nil {
		mr.Close()
		t.Fatalf("Failed to connect to miniredis: %v", err)
	}
	t.Cleanup(func() {
		rdb.Close()
		mr.Close()
	})
	return mr, redis.NewPresenceStore(rdb), redis.NewPublisher(rdb)
}

func TestPresenceRegistrationAndHeartbeat(t *testing.T) {
	_, store, pub := setupMiniredis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr := presence.NewManager(store, pub)
	hb := presence.NewHeartbeatHandler(store)

	docID := "test_presence_doc_1"
	userID := "user_presence_test"

	// 1. Register Presence
	online, err := mgr.RegisterUser(ctx, docID, userID, "TEST-SERVER")
	if err != nil {
		t.Fatalf("Failed to register user: %v", err)
	}
	if len(online) == 0 {
		t.Fatalf("Expected online users to include %s", userID)
	}

	// Verify in Redis
	users, err := store.GetOnlineUsers(ctx, docID, time.Now().Unix()-30)
	if err != nil {
		t.Fatalf("Failed to get online users from Redis: %v", err)
	}
	if len(users) != 1 || users[0] != userID {
		t.Errorf("Expected online user [%s], got %v", userID, users)
	}

	// 2. Test Heartbeat
	futureTime := time.Now().Unix() + 10
	if err := store.AddOrUpdateUser(ctx, docID, userID, futureTime); err != nil {
		t.Fatalf("Failed to execute heartbeat: %v", err)
	}

	if err := hb.ProcessHeartbeat(ctx, docID, userID); err != nil {
		t.Fatalf("Heartbeat handler returned error: %v", err)
	}

	// Cleanup
	_ = mgr.UnregisterUser(ctx, docID, userID, "TEST-SERVER")
}

func TestOfflineUserDetection(t *testing.T) {
	_, store, pub := setupMiniredis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := presence.NewCleanupWorker(store, pub)

	docID := "test_offline_doc"
	oldUser := "stale_user"
	activeUser := "active_user"

	// Old user last seen 60 seconds ago
	_ = store.AddOrUpdateUser(ctx, docID, oldUser, time.Now().Unix()-60)
	// Active user last seen now
	_ = store.AddOrUpdateUser(ctx, docID, activeUser, time.Now().Unix())

	// Run cleanup pass
	worker.RunCleanup(ctx)

	// Verify stale_user was removed, active_user remains
	online, err := store.GetOnlineUsers(ctx, docID, time.Now().Unix()-30)
	if err != nil {
		t.Fatalf("Failed to get online users: %v", err)
	}

	for _, u := range online {
		if u == oldUser {
			t.Errorf("Expected stale user %s to be evicted, but was found online", oldUser)
		}
	}

	// Clean up remaining user
	_ = store.RemoveUser(ctx, docID, activeUser)
}

func TestCursorAndTypingIndicators(t *testing.T) {
	_, store, pub := setupMiniredis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	curTracker := cursor.NewTracker(store, pub)
	typIndicator := typing.NewIndicator(store, pub)

	docID := "test_cursor_doc"
	userID := "cursor_user"

	// 1. Cursor update
	pos := &messaging.Position{Line: 10, Column: 5}
	if err := curTracker.UpdateCursor(ctx, docID, userID, "TEST-SERVER", pos); err != nil {
		t.Fatalf("Failed to update cursor: %v", err)
	}

	cursors, err := curTracker.GetCursors(ctx, docID)
	if err != nil {
		t.Fatalf("Failed to get cursors: %v", err)
	}
	if cursors[userID] == "" {
		t.Errorf("Expected cursor position for %s, got empty", userID)
	}

	// 2. Typing indicator (start typing)
	if err := typIndicator.SetTyping(ctx, docID, userID, "TEST-SERVER", true); err != nil {
		t.Fatalf("Failed to set typing true: %v", err)
	}

	isTyping, err := typIndicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing state: %v", err)
	}
	if !isTyping {
		t.Errorf("Expected user %s to be typing", userID)
	}

	// 3. Typing indicator (stop typing)
	if err := typIndicator.SetTyping(ctx, docID, userID, "TEST-SERVER", false); err != nil {
		t.Fatalf("Failed to set typing false: %v", err)
	}

	isTyping, err = typIndicator.IsTyping(ctx, docID, userID)
	if err != nil {
		t.Fatalf("Failed to check typing state: %v", err)
	}
	if isTyping {
		t.Errorf("Expected user %s to stop typing, but typing key was found", userID)
	}

	// Clean up
	_ = store.RemoveUser(ctx, docID, userID)
}

func TestMultiServerPresenceBroadcast(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb1, err := redis.NewClient(mr.Addr())
	if err != nil {
		t.Fatalf("Failed to connect rdb1: %v", err)
	}
	defer rdb1.Close()

	rdb2, err := redis.NewClient(mr.Addr())
	if err != nil {
		t.Fatalf("Failed to connect rdb2: %v", err)
	}
	defer rdb2.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store1 := redis.NewPresenceStore(rdb1)
	store2 := redis.NewPresenceStore(rdb2)

	pub1 := redis.NewPublisher(rdb1)
	pub2 := redis.NewPublisher(rdb2)

	sub1 := redis.NewSubscriber(rdb1, "SERVER-1")
	sub2 := redis.NewSubscriber(rdb2, "SERVER-2")

	hub1 := websocket.NewHub("SERVER-1", pub1)
	hub2 := websocket.NewHub("SERVER-2", pub2)

	hub1.SetPresenceServices(
		presence.NewManager(store1, pub1),
		presence.NewHeartbeatHandler(store1),
		cursor.NewTracker(store1, pub1),
		typing.NewIndicator(store1, pub1),
	)

	hub2.SetPresenceServices(
		presence.NewManager(store2, pub2),
		presence.NewHeartbeatHandler(store2),
		cursor.NewTracker(store2, pub2),
		typing.NewIndicator(store2, pub2),
	)

	go hub1.Run(ctx)
	go hub2.Run(ctx)

	go func() { _ = sub1.StartListening(ctx, hub1.HandleRedisEvent) }()
	go func() { _ = sub2.StartListening(ctx, hub2.HandleRedisEvent) }()

	time.Sleep(100 * time.Millisecond)

	httpServer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWs(hub1, w, r)
	}))
	defer httpServer1.Close()

	httpServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWs(hub2, w, r)
	}))
	defer httpServer2.Close()

	wsURL1 := "ws" + strings.TrimPrefix(httpServer1.URL, "http") + "/doc_presence_multi?userId=user_A"
	wsURL2 := "ws" + strings.TrimPrefix(httpServer2.URL, "http") + "/doc_presence_multi?userId=user_B"

	dialer := gorilla.Dialer{}
	conn1, _, err := dialer.Dial(wsURL1, nil)
	if err != nil {
		t.Fatalf("Failed to connect User A: %v", err)
	}
	defer conn1.Close()

	conn2, _, err := dialer.Dial(wsURL2, nil)
	if err != nil {
		t.Fatalf("Failed to connect User B: %v", err)
	}
	defer conn2.Close()

	time.Sleep(150 * time.Millisecond)

	// User A on Server 1 sends a cursor move
	cursorEvt := messaging.Event{
		Type:       messaging.EventTypeCursorMove,
		DocumentID: "doc_presence_multi",
		UserID:     "user_A",
		Position:   &messaging.Position{Line: 10, Column: 5},
	}

	if err := conn1.WriteJSON(cursorEvt); err != nil {
		t.Fatalf("Failed to send cursor event from User A: %v", err)
	}

	// User B on Server 2 should receive the cursor move event
	var recvOnB messaging.Event
	deadline := time.Now().Add(3 * time.Second)
	_ = conn2.SetReadDeadline(deadline)
	for {
		if err := conn2.ReadJSON(&recvOnB); err != nil {
			t.Fatalf("User B on Server 2 failed to receive cursor event: %v", err)
		}
		if recvOnB.Type == messaging.EventTypeCursorMove {
			break
		}
	}

	if recvOnB.UserID != "user_A" {
		t.Errorf("Expected UserID 'user_A', got '%s'", recvOnB.UserID)
	}
	if recvOnB.Type != messaging.EventTypeCursorMove {
		t.Errorf("Expected EventType 'cursor_move', got '%s'", recvOnB.Type)
	}
}
