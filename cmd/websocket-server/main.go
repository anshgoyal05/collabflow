package main

import (
	"log"
	"net/http"
	"os"

	"collabflow/internal/websocket"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting CollabFlow WebSocket server on port %s...", port)

	hub := websocket.NewHub()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWs(hub, w, r)
	})

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatalf("ListenAndServe failed: %v", err)
	}
}
