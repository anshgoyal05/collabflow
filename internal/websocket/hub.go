package websocket

import (
	"context"
	"log"
	"sync"
	"time"

	"collabflow/internal/cursor"
	"collabflow/internal/messaging"
	"collabflow/internal/presence"
	"collabflow/internal/redis"
	"collabflow/internal/typing"
)

// Hub maintains the set of active clients per document room and forwards messages to Redis.
type Hub struct {
	serverID  string
	publisher *redis.Publisher

	presenceMgr      *presence.Manager
	heartbeatHandler *presence.HeartbeatHandler
	cursorTracker    *cursor.Tracker
	typingIndicator  *typing.Indicator

	// Registered clients across all rooms.
	clients map[*Client]bool

	// Document rooms: documentID -> map[*Client]bool
	rooms map[string]map[*Client]bool

	// Inbound messages from clients to be published to Redis.
	inbound chan messaging.Event

	// Incoming messages from Redis Pub/Sub to be broadcast to local clients.
	redisEvent chan messaging.Event

	// Register requests from clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	mu sync.RWMutex
}

// NewHub creates and returns a new Hub instance.
func NewHub(serverID string, publisher *redis.Publisher) *Hub {
	return &Hub{
		serverID:   serverID,
		publisher:  publisher,
		clients:    make(map[*Client]bool),
		rooms:      make(map[string]map[*Client]bool),
		inbound:    make(chan messaging.Event, 256),
		redisEvent: make(chan messaging.Event, 256),
		register:   make(chan *Client, 64),
		unregister: make(chan *Client, 64),
	}
}

// SetPresenceServices configures the presence, heartbeat, cursor, and typing services for Hub.
func (h *Hub) SetPresenceServices(
	presenceMgr *presence.Manager,
	heartbeatHandler *presence.HeartbeatHandler,
	cursorTracker *cursor.Tracker,
	typingIndicator *typing.Indicator,
) {
	h.presenceMgr = presenceMgr
	h.heartbeatHandler = heartbeatHandler
	h.cursorTracker = cursorTracker
	h.typingIndicator = typingIndicator
}

// ServerID returns the unique identifier for this server instance.
func (h *Hub) ServerID() string {
	return h.serverID
}

// HandleRedisEvent processes an incoming event from the Redis subscriber.
func (h *Hub) HandleRedisEvent(documentID string, event messaging.Event) {
	h.redisEvent <- event
}

// Run starts the event loop of the Hub.
func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			if client.documentID != "" {
				if _, ok := h.rooms[client.documentID]; !ok {
					h.rooms[client.documentID] = make(map[*Client]bool)
				}
				h.rooms[client.documentID][client] = true
			}
			h.mu.Unlock()
			log.Printf("[%s]\nUser connected:\nuser_id=%s\ndocument=%s", h.serverID, client.userID, client.documentID)

			if h.presenceMgr != nil {
				go func(c *Client) {
					onlineUsers, err := h.presenceMgr.RegisterUser(ctx, c.documentID, c.userID, h.serverID)
					if err != nil {
						log.Printf("[%s] Error registering presence for user %s: %v", h.serverID, c.userID, err)
					}
					// Send initial presence_update directly to newly connected client
					initEvt := messaging.Event{
						Type:        messaging.EventTypePresenceUpdate,
						DocumentID:  c.documentID,
						UserID:      c.userID,
						OnlineUsers: onlineUsers,
						ServerID:    h.serverID,
						Timestamp:   time.Now().Unix(),
					}
					select {
					case c.send <- initEvt:
					default:
					}
				}(client)
			}

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				if client.documentID != "" && h.rooms[client.documentID] != nil {
					delete(h.rooms[client.documentID], client)
					if len(h.rooms[client.documentID]) == 0 {
						delete(h.rooms, client.documentID)
					}
				}
				close(client.send)
				log.Printf("[%s]\nUser disconnected:\nuser_id=%s\ndocument=%s", h.serverID, client.userID, client.documentID)

				if h.presenceMgr != nil {
					go func(docID, userID string) {
						if err := h.presenceMgr.UnregisterUser(ctx, docID, userID, h.serverID); err != nil {
							log.Printf("[%s] Error unregistering presence for user %s: %v", h.serverID, userID, err)
						}
					}(client.documentID, client.userID)
				}
			}
			h.mu.Unlock()

		case msg := <-h.inbound:
			if msg.ServerID == "" {
				msg.ServerID = h.serverID
			}

			// Handle presence-specific event types
			switch msg.Type {
			case messaging.EventTypeJoinDocument:
				if h.presenceMgr != nil {
					go func(m messaging.Event) {
						onlineUsers, _ := h.presenceMgr.RegisterUser(ctx, m.DocumentID, m.UserID, h.serverID)
						resp := messaging.Event{
							Type:        messaging.EventTypePresenceUpdate,
							DocumentID:  m.DocumentID,
							OnlineUsers: onlineUsers,
							ServerID:    h.serverID,
							Timestamp:   time.Now().Unix(),
						}
						if h.publisher != nil {
							_ = h.publisher.Publish(ctx, m.DocumentID, resp)
						} else {
							h.broadcastLocal(m.DocumentID, resp)
						}
					}(msg)
				}

			case messaging.EventTypeHeartbeat:
				if h.heartbeatHandler != nil {
					go func(m messaging.Event) {
						_ = h.heartbeatHandler.ProcessHeartbeat(ctx, m.DocumentID, m.UserID)
					}(msg)
				}

			case messaging.EventTypeCursorMove, messaging.EventTypeCursorUpdate:
				if h.cursorTracker != nil {
					go func(m messaging.Event) {
						_ = h.cursorTracker.UpdateCursor(ctx, m.DocumentID, m.UserID, h.serverID, m.Position)
					}(msg)
				} else {
					h.publishOrBroadcast(ctx, msg)
				}

			case messaging.EventTypeTyping, messaging.EventTypeTypingStart:
				if h.typingIndicator != nil {
					go func(m messaging.Event) {
						status := true
						if m.Status != nil {
							status = *m.Status
						}
						_ = h.typingIndicator.SetTyping(ctx, m.DocumentID, m.UserID, h.serverID, status)
					}(msg)
				} else {
					h.publishOrBroadcast(ctx, msg)
				}

			default:
				h.publishOrBroadcast(ctx, msg)
			}

		case msg := <-h.redisEvent:
			h.broadcastLocal(msg.DocumentID, msg)
		}
	}
}

func (h *Hub) publishOrBroadcast(ctx context.Context, msg messaging.Event) {
	log.Printf("[%s]\nPublished:\ndocument=%s", h.serverID, msg.DocumentID)
	if h.publisher != nil {
		go func(m messaging.Event) {
			if err := h.publisher.Publish(ctx, m.DocumentID, m); err != nil {
				log.Printf("[%s] Failed to publish event to Redis: %v", h.serverID, err)
			}
		}(msg)
	} else {
		h.broadcastLocal(msg.DocumentID, msg)
	}
}

func (h *Hub) broadcastLocal(documentID string, msg messaging.Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	roomClients := h.rooms[documentID]
	userCount := len(roomClients)
	log.Printf("[%s]\n\nRedis event received:\ndocument=%s\n\nBroadcasted:\nusers=%d", h.serverID, documentID, userCount)

	if userCount == 0 {
		return
	}

	for client := range roomClients {
		select {
		case client.send <- msg:
		default:
			close(client.send)
			delete(h.clients, client)
			delete(h.rooms[documentID], client)
		}
	}
}

// GetClients returns a slice of all currently registered clients.
func (h *Hub) GetClients() []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()
	list := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		list = append(list, client)
	}
	return list
}

