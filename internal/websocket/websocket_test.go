package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"collabflow/internal/models"
	"github.com/gorilla/websocket"
)

func TestWebSocketBroadcast(t *testing.T) {
	// Initialize hub
	hub := NewHub()
	go hub.Run()

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeWs(hub, w, r)
	}))
	defer server.Close()

	// Convert http:// to ws://
	url := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?userId=testUser1"
	url2 := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws?userId=testUser2"

	// Connect Client 1
	dialer := websocket.Dialer{}
	conn1, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("Failed to dial client 1: %v", err)
	}
	defer conn1.Close()

	// Connect Client 2
	conn2, _, err := dialer.Dial(url2, nil)
	if err != nil {
		t.Fatalf("Failed to dial client 2: %v", err)
	}
	defer conn2.Close()

	// Give a small grace period for the registration channels to process
	time.Sleep(100 * time.Millisecond)

	// Verify clients registered in hub
	clients := hub.GetClients()
	if len(clients) != 2 {
		t.Errorf("Expected 2 registered clients, got %d", len(clients))
	}

	// Send message from Client 1
	testMsg := models.Message{
		Type:       "insert",
		UserID:     "testUser1",
		DocumentID: "doc-abc",
		Content:    "Hello World",
	}

	if err := conn1.WriteJSON(testMsg); err != nil {
		t.Fatalf("Failed to write JSON from client 1: %v", err)
	}

	// Receive message on Client 2
	var recvMsg2 models.Message
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn2.ReadJSON(&recvMsg2); err != nil {
		t.Fatalf("Client 2 failed to read broadcasted JSON: %v", err)
	}

	if recvMsg2.UserID != "testUser1" {
		t.Errorf("Expected UserID testUser1, got %s", recvMsg2.UserID)
	}
	if recvMsg2.Content != "Hello World" {
		t.Errorf("Expected Content 'Hello World', got '%s'", recvMsg2.Content)
	}

	// Receive message on Client 1 (since it was broadcast to all)
	var recvMsg1 models.Message
	_ = conn1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if err := conn1.ReadJSON(&recvMsg1); err != nil {
		t.Fatalf("Client 1 failed to read broadcasted JSON: %v", err)
	}

	if recvMsg1.Content != "Hello World" {
		t.Errorf("Expected Content 'Hello World' on client 1, got '%s'", recvMsg1.Content)
	}
}
