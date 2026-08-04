package websocket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"collabflow/internal/messaging"
	"collabflow/internal/redis"
	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
)

func TestWebSocketBroadcastLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize hub without Redis (standalone mode)
	hub := NewHub("SERVER-TEST", nil)
	go hub.Run(ctx)

	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, w, r)
	}))
	defer server.Close()

	wsURL1 := "ws" + strings.TrimPrefix(server.URL, "http") + "/doc_123?userId=testUser1"
	wsURL2 := "ws" + strings.TrimPrefix(server.URL, "http") + "/doc_123?userId=testUser2"

	dialer := websocket.Dialer{}
	conn1, _, err := dialer.Dial(wsURL1, nil)
	if err != nil {
		t.Fatalf("Failed to dial client 1: %v", err)
	}
	defer conn1.Close()

	conn2, _, err := dialer.Dial(wsURL2, nil)
	if err != nil {
		t.Fatalf("Failed to dial client 2: %v", err)
	}
	defer conn2.Close()

	time.Sleep(100 * time.Millisecond)

	clients := hub.GetClients()
	if len(clients) != 2 {
		t.Errorf("Expected 2 registered clients, got %d", len(clients))
	}

	testMsg := messaging.Event{
		Type:       "insert",
		UserID:     "testUser1",
		DocumentID: "doc_123",
		Content:    "Hello World Local",
	}

	if err := conn1.WriteJSON(testMsg); err != nil {
		t.Fatalf("Failed to write JSON from client 1: %v", err)
	}

	var recvMsg2 messaging.Event
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn2.ReadJSON(&recvMsg2); err != nil {
		t.Fatalf("Client 2 failed to read broadcasted JSON: %v", err)
	}

	if recvMsg2.UserID != "testUser1" {
		t.Errorf("Expected UserID testUser1, got %s", recvMsg2.UserID)
	}
	if recvMsg2.GetContent() != "Hello World Local" {
		t.Errorf("Expected Content 'Hello World Local', got '%s'", recvMsg2.GetContent())
	}
}

func TestMultiServerRedisBroadcast(t *testing.T) {
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

	pub1 := redis.NewPublisher(rdb1)
	pub2 := redis.NewPublisher(rdb2)

	sub1 := redis.NewSubscriber(rdb1, "SERVER-1")
	sub2 := redis.NewSubscriber(rdb2, "SERVER-2")

	hub1 := NewHub("SERVER-1", pub1)
	hub2 := NewHub("SERVER-2", pub2)

	go hub1.Run(ctx)
	go hub2.Run(ctx)

	go func() { _ = sub1.StartListening(ctx, hub1.HandleRedisEvent) }()
	go func() { _ = sub2.StartListening(ctx, hub2.HandleRedisEvent) }()

	time.Sleep(100 * time.Millisecond)

	httpServer1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub1, w, r)
	}))
	defer httpServer1.Close()

	httpServer2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub2, w, r)
	}))
	defer httpServer2.Close()

	wsURL1 := "ws" + strings.TrimPrefix(httpServer1.URL, "http") + "/doc_cross?userId=user_A"
	wsURL2 := "ws" + strings.TrimPrefix(httpServer2.URL, "http") + "/doc_cross?userId=user_B"

	dialer := websocket.Dialer{}
	conn1, _, err := dialer.Dial(wsURL1, nil)
	if err != nil {
		t.Fatalf("Failed to connect User A to Server 1: %v", err)
	}
	defer conn1.Close()

	conn2, _, err := dialer.Dial(wsURL2, nil)
	if err != nil {
		t.Fatalf("Failed to connect User B to Server 2: %v", err)
	}
	defer conn2.Close()

	time.Sleep(150 * time.Millisecond)

	msgToSend := messaging.Event{
		Type:       "insert",
		DocumentID: "doc_cross",
		UserID:     "user_A",
		Position:   &messaging.Position{Index: 5},
		Content:    "Hello from Server 1",
	}

	if err := conn1.WriteJSON(msgToSend); err != nil {
		t.Fatalf("Failed to send edit from User A on Server 1: %v", err)
	}

	var recvOnB messaging.Event
	_ = conn2.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := conn2.ReadJSON(&recvOnB); err != nil {
		t.Fatalf("User B on Server 2 failed to receive cross-server message: %v", err)
	}

	if recvOnB.UserID != "user_A" {
		t.Errorf("Expected UserID 'user_A', got '%s'", recvOnB.UserID)
	}
	if recvOnB.GetContent() != "Hello from Server 1" {
		t.Errorf("Expected Content 'Hello from Server 1', got '%s'", recvOnB.GetContent())
	}
}
