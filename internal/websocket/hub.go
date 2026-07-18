package websocket

import (
	"log"
	"sync"

	"collabflow/internal/models"
)

// Hub maintains the set of active clients and broadcasts messages to the clients.
type Hub struct {
	// Registered clients.
	clients map[*Client]bool

	// Inbound messages from the clients.
	broadcast chan models.Message

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	mu sync.RWMutex
}

// NewHub creates and returns a new Hub instance.
func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan models.Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

// Run starts the main loop of the hub to process register, unregister and broadcast events.
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("Client registered: %s", client.userID)

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("Client unregistered: %s", client.userID)
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					h.mu.Lock()
					delete(h.clients, client)
					h.mu.Unlock()
				}
			}
			h.mu.RUnlock()
		}
	}
}

// GetClients returns a list of currently active clients.
func (h *Hub) GetClients() []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	list := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		list = append(list, client)
	}
	return list
}
