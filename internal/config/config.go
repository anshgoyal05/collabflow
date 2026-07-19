package config

import (
	"os"
)

// Config holds all configuration parameters for the WebSocket server.
type Config struct {
	Port      string
	RedisAddr string
	ServerID  string
}

// LoadConfig reads configuration from environment variables or applies defaults.
func LoadConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "SERVER-1"
	}

	return &Config{
		Port:      port,
		RedisAddr: redisAddr,
		ServerID:  serverID,
	}
}
