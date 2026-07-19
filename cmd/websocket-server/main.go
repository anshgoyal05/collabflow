package main

import (
	"context"
	"log"
	"net/http"

	"collabflow/internal/config"
	"collabflow/internal/redis"
	"collabflow/internal/websocket"
)

func main() {
	cfg := config.LoadConfig()

	log.Printf("[%s] Starting CollabFlow WebSocket server on port %s...", cfg.ServerID, cfg.Port)
	log.Printf("[%s] Connecting to Redis at %s...", cfg.ServerID, cfg.RedisAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var publisher *redis.Publisher
	var subscriber *redis.Subscriber

	rdb, err := redis.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Printf("[%s] WARNING: Redis connection failed (%v). Running in standalone memory mode.", cfg.ServerID, err)
	} else {
		defer rdb.Close()
		publisher = redis.NewPublisher(rdb)
		subscriber = redis.NewSubscriber(rdb, cfg.ServerID)
	}

	// Create and start Hub
	hub := websocket.NewHub(cfg.ServerID, publisher)

	if subscriber != nil {
		go func() {
			if err := subscriber.StartListening(ctx, hub.HandleRedisEvent); err != nil {
				log.Printf("[%s] Redis subscriber error: %v", cfg.ServerID, err)
			}
		}()
	}

	go hub.Run(ctx)

	// Handle WebSocket connections on all path routes (/ws, /ws/{docId}, /{docId})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		websocket.ServeWs(hub, w, r)
	})

	log.Printf("[%s] Server initialized successfully. Listening on :%s", cfg.ServerID, cfg.Port)
	err = http.ListenAndServe(":"+cfg.Port, nil)
	if err != nil {
		log.Fatalf("[%s] ListenAndServe failed: %v", cfg.ServerID, err)
	}
}
