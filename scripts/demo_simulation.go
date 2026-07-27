package main

import (
	"fmt"
	"log"
	"time"

	gorilla "github.com/gorilla/websocket"
)

type Event struct {
	Type        string      `json:"type"`
	DocumentID  string      `json:"documentId,omitempty"`
	UserID      string      `json:"userId,omitempty"`
	Position    interface{} `json:"position,omitempty"`
	Content     string      `json:"content,omitempty"`
	OnlineUsers []string    `json:"onlineUsers,omitempty"`
	ServerID    string      `json:"serverId,omitempty"`
}

func main() {
	fmt.Println("🚀 Starting Automated CollabFlow Real-Time Presence & Sync Demo...")

	url1 := "ws://localhost:8081/doc_demo?userId=User_Alice"
	url2 := "ws://localhost:8082/doc_demo?userId=User_Bob"

	dialer := gorilla.Dialer{}

	// Connect Alice to Server 1
	fmt.Println("Connecting Alice to Server 1 (port 8081)...")
	conn1, _, err := dialer.Dial(url1, nil)
	if err != nil {
		log.Fatalf("Failed to connect Alice: %v", err)
	}
	defer conn1.Close()

	// Connect Bob to Server 2
	fmt.Println("Connecting Bob to Server 2 (port 8082)...")
	conn2, _, err := dialer.Dial(url2, nil)
	if err != nil {
		log.Fatalf("Failed to connect Bob: %v", err)
	}
	defer conn2.Close()

	time.Sleep(300 * time.Millisecond)

	// Listen for events on Bob (Server 2)
	go func() {
		for {
			var evt Event
			if err := conn2.ReadJSON(&evt); err != nil {
				return
			}
			fmt.Printf("   📩 [Bob on Server 2 Received] Type: %-15s | User: %-10s | Server: %-8s | Content/Users: %v\n",
				evt.Type, evt.UserID, evt.ServerID, getDisplayData(evt))
		}
	}()

	time.Sleep(300 * time.Millisecond)

	// 1. Send Heartbeat from Alice
	fmt.Println("\n1️⃣ Alice sends Heartbeat to Server 1...")
	_ = conn1.WriteJSON(Event{Type: "heartbeat", UserID: "User_Alice", DocumentID: "doc_demo"})
	time.Sleep(500 * time.Millisecond)

	// 2. Send Cursor Movement from Alice
	fmt.Println("\n2️⃣ Alice moves cursor (Line 12, Col 8)...")
	_ = conn1.WriteJSON(Event{
		Type:       "cursor_move",
		UserID:     "User_Alice",
		DocumentID: "doc_demo",
		Position:   map[string]int{"line": 12, "column": 8},
	})
	time.Sleep(500 * time.Millisecond)

	// 3. Send Typing Indicator from Alice
	fmt.Println("\n3️⃣ Alice starts typing...")
	_ = conn1.WriteJSON(Event{Type: "typing_start", UserID: "User_Alice", DocumentID: "doc_demo"})
	time.Sleep(500 * time.Millisecond)

	// 4. Send Document Edit from Alice
	fmt.Println("\n4️⃣ Alice inserts text 'Real-time presence works!'...")
	_ = conn1.WriteJSON(Event{
		Type:       "insert",
		UserID:     "User_Alice",
		DocumentID: "doc_demo",
		Position:   0,
		Content:    "Real-time presence works!",
	})
	time.Sleep(1 * time.Second)

	fmt.Println("\n✅ Automated Demo Completed Successfully!")
}

func getDisplayData(evt Event) interface{} {
	if len(evt.OnlineUsers) > 0 {
		return evt.OnlineUsers
	}
	if evt.Content != "" {
		return evt.Content
	}
	if evt.Position != nil {
		return evt.Position
	}
	return "ok"
}
