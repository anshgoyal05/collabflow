package websocket

import (
	"log"
	"time"

	"collabflow/internal/messaging"
	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 4096
)

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	hub *Hub

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan messaging.Event

	// Unique identifier for the client.
	userID string

	// Document ID room this client belongs to.
	documentID string
}

// NewClient creates a new client instance.
func NewClient(hub *Hub, conn *websocket.Conn, userID, documentID string) *Client {
	return &Client{
		hub:        hub,
		conn:       conn,
		send:       make(chan messaging.Event, 256),
		userID:     userID,
		documentID: documentID,
	}
}

// UserID returns the client's user ID.
func (c *Client) UserID() string {
	return c.userID
}

// DocumentID returns the client's document ID.
func (c *Client) DocumentID() string {
	return c.documentID
}

// ReadPump pumps messages from the websocket connection to the hub.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		var msg messaging.Event
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket read error: %v", err)
			}
			break
		}

		// Fill missing fields with client metadata
		if msg.UserID == "" {
			msg.UserID = c.userID
		}
		if msg.DocumentID == "" {
			msg.DocumentID = c.documentID
		}
		if msg.ServerID == "" {
			msg.ServerID = c.hub.ServerID()
		}

		// Pass message to hub to publish via Redis
		c.hub.inbound <- msg
	}
}

// WritePump pumps messages from the hub to the websocket connection.
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			err := c.conn.WriteJSON(message)
			if err != nil {
				log.Printf("websocket write error: %v", err)
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
