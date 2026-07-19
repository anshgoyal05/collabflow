package websocket

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all connections for development.
		return true
	},
}

// GenerateRandomID creates a secure random hexadecimal string of length 16.
func GenerateRandomID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "anonymous"
	}
	return hex.EncodeToString(bytes)
}

// ServeWs handles websocket requests from the client.
func ServeWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Websocket upgrade failure: %v", err)
		return
	}

	userID := r.URL.Query().Get("userId")
	if userID == "" {
		userID = GenerateRandomID()
	}

	documentID := r.URL.Query().Get("documentId")
	if documentID == "" {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if strings.HasPrefix(path, "ws/") {
			path = strings.TrimPrefix(path, "ws/")
		} else if path == "ws" {
			path = ""
		}
		if path != "" {
			documentID = path
		}
	}
	if documentID == "" {
		documentID = "default"
	}

	client := NewClient(hub, conn, userID, documentID)
	hub.register <- client

	go client.WritePump()
	go client.ReadPump()
}
