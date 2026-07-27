package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// PresenceStore handles all Redis interactions for temporary presence, cursors, and typing state.
type PresenceStore struct {
	rdb *redis.Client
}

// NewPresenceStore initializes a new PresenceStore.
func NewPresenceStore(rdb *redis.Client) *PresenceStore {
	return &PresenceStore{rdb: rdb}
}

// GetPresenceKey formats the Redis key for presence sorted set.
func GetPresenceKey(docID string) string {
	return fmt.Sprintf("presence:%s", docID)
}

// GetCursorKey formats the Redis key for cursor hash.
func GetCursorKey(docID string) string {
	return fmt.Sprintf("cursor:%s", docID)
}

// GetTypingKey formats the Redis key for typing indicator.
func GetTypingKey(docID, userID string) string {
	return fmt.Sprintf("typing:%s:%s", docID, userID)
}

// AddOrUpdateUser adds or updates a user's presence score (timestamp in seconds) in the Sorted Set.
func (p *PresenceStore) AddOrUpdateUser(ctx context.Context, docID, userID string, timestamp int64) error {
	key := GetPresenceKey(docID)
	err := p.rdb.ZAdd(ctx, key, redis.Z{
		Score:  float64(timestamp),
		Member: userID,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to add/update presence for user %s in %s: %w", userID, docID, err)
	}
	return nil
}

// GetOnlineUsers retrieves all user IDs in a document whose last heartbeat is >= minScore.
func (p *PresenceStore) GetOnlineUsers(ctx context.Context, docID string, minScore int64) ([]string, error) {
	key := GetPresenceKey(docID)
	users, err := p.rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", minScore),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get online users for %s: %w", docID, err)
	}
	return users, nil
}

// RemoveUser removes a user from presence sorted set and cursor hash.
func (p *PresenceStore) RemoveUser(ctx context.Context, docID, userID string) error {
	presKey := GetPresenceKey(docID)
	curKey := GetCursorKey(docID)

	pipe := p.rdb.Pipeline()
	pipe.ZRem(ctx, presKey, userID)
	pipe.HDel(ctx, curKey, userID)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove user %s from %s: %w", userID, docID, err)
	}
	return nil
}

// GetActiveDocumentIDs scans and returns all document IDs with presence tracking.
func (p *PresenceStore) GetActiveDocumentIDs(ctx context.Context) ([]string, error) {
	var docIDs []string
	var cursor uint64
	for {
		keys, nextCursor, err := p.rdb.Scan(ctx, cursor, "presence:*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("failed to scan presence keys: %w", err)
		}
		for _, k := range keys {
			docIDs = append(docIDs, k[len("presence:"):])
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return docIDs, nil
}

// CleanupOfflineUsers removes users whose last seen timestamp is < cutoffTimestamp from presence and cursor store.
// Returns the list of evicted user IDs.
func (p *PresenceStore) CleanupOfflineUsers(ctx context.Context, docID string, cutoffTimestamp int64) ([]string, error) {
	key := GetPresenceKey(docID)
	// Find users to be removed
	expiredUsers, err := p.rdb.ZRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("(%d", cutoffTimestamp),
	}).Result()

	if err != nil || len(expiredUsers) == 0 {
		return nil, err
	}

	curKey := GetCursorKey(docID)
	pipe := p.rdb.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("(%d", cutoffTimestamp))
	for _, user := range expiredUsers {
		pipe.HDel(ctx, curKey, user)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to cleanup offline users for %s: %w", docID, err)
	}

	return expiredUsers, nil
}

// SetCursor stores user cursor position JSON in Redis Hash.
func (p *PresenceStore) SetCursor(ctx context.Context, docID, userID, positionJSON string) error {
	key := GetCursorKey(docID)
	if err := p.rdb.HSet(ctx, key, userID, positionJSON).Err(); err != nil {
		return fmt.Errorf("failed to set cursor for user %s in %s: %w", userID, docID, err)
	}
	return nil
}

// GetCursors retrieves all cursor positions for a document.
func (p *PresenceStore) GetCursors(ctx context.Context, docID string) (map[string]string, error) {
	key := GetCursorKey(docID)
	result, err := p.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cursors for %s: %w", docID, err)
	}
	return result, nil
}

// SetTyping sets typing status key with TTL (e.g., 3 seconds).
func (p *PresenceStore) SetTyping(ctx context.Context, docID, userID string, ttl time.Duration) error {
	key := GetTypingKey(docID, userID)
	if err := p.rdb.Set(ctx, key, "1", ttl).Err(); err != nil {
		return fmt.Errorf("failed to set typing state for user %s in %s: %w", userID, docID, err)
	}
	return nil
}

// IsTyping checks if typing key exists.
func (p *PresenceStore) IsTyping(ctx context.Context, docID, userID string) (bool, error) {
	key := GetTypingKey(docID, userID)
	n, err := p.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check typing state for user %s in %s: %w", userID, docID, err)
	}
	return n > 0, nil
}
