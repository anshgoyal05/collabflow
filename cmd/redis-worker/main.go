package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"collabflow/internal/config"
	"collabflow/internal/messaging"
	"collabflow/internal/redis"
)

func main() {
	cfg := config.LoadConfig()
	log.Printf("[REDIS-WORKER] Starting Redis Worker daemon, target Redis: %s", cfg.RedisAddr)

	rdb, err := redis.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("[REDIS-WORKER] Fatal error connecting to Redis: %v", err)
	}
	defer rdb.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subscriber := redis.NewSubscriber(rdb, "REDIS-WORKER")

	go func() {
		log.Printf("[REDIS-WORKER] Subscribed to pattern `document:*`. Listening for real-time document events...")
		err := subscriber.StartListening(ctx, func(docID string, event messaging.Event) {
			log.Printf("[REDIS-WORKER] Event Logged -> Doc: %s | Type: %s | User: %s | Server: %s | Content: %s",
				docID, event.Type, event.UserID, event.ServerID, event.GetContent())
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("[REDIS-WORKER] Subscriber exited with error: %v", err)
		}
	}()

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Printf("[REDIS-WORKER] Shutting down Redis Worker daemon...")
}
