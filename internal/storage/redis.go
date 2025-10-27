package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/types"
	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &RedisStore{client: client}, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) AddJob(ctx context.Context, job *types.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshaling job: %w", err)
	}

	normalizedRules := types.NormalizeQueryRules(job.AgentQueryRules)
	key := fmt.Sprintf("jobs:%s", normalizedRules)

	if err := s.client.RPush(ctx, key, data).Err(); err != nil {
		return fmt.Errorf("adding job to redis: %w", err)
	}

	if err := s.client.Expire(ctx, key, 1*time.Hour).Err(); err != nil {
		return fmt.Errorf("setting expiry: %w", err)
	}

	metaKey := fmt.Sprintf("job:%s", job.UUID)
	if err := s.client.HSet(ctx, metaKey,
		"queue_key", job.QueueKey,
		"query_rules", normalizedRules,
		"reserved_at", job.ReservedAt.Format(time.RFC3339),
		"status", "reserved",
	).Err(); err != nil {
		return fmt.Errorf("setting job metadata: %w", err)
	}

	if err := s.client.Expire(ctx, metaKey, 1*time.Hour).Err(); err != nil {
		return fmt.Errorf("setting metadata expiry: %w", err)
	}

	return nil
}

func (s *RedisStore) ClaimJob(ctx context.Context, queryRules []string) (*types.Job, error) {
	normalizedRules := types.NormalizeQueryRules(queryRules)
	key := fmt.Sprintf("jobs:%s", normalizedRules)

	data, err := s.client.LPop(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("popping job from redis: %w", err)
	}

	var job types.Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("unmarshaling job: %w", err)
	}

	metaKey := fmt.Sprintf("job:%s", job.UUID)
	if err := s.client.HSet(ctx, metaKey, "status", "claimed").Err(); err != nil {
		return nil, fmt.Errorf("updating job status: %w", err)
	}

	return &job, nil
}

func (s *RedisStore) CompleteJob(ctx context.Context, uuid string) error {
	metaKey := fmt.Sprintf("job:%s", uuid)
	if err := s.client.HSet(ctx, metaKey, "status", "complete").Err(); err != nil {
		return fmt.Errorf("updating job status: %w", err)
	}
	return nil
}

func (s *RedisStore) GetQueueStats(ctx context.Context, queryRules string) (int64, error) {
	key := fmt.Sprintf("jobs:%s", queryRules)
	return s.client.LLen(ctx, key).Result()
}

func (s *RedisStore) GetAllStats(ctx context.Context) (map[string]int64, error) {
	keys, err := s.client.Keys(ctx, "jobs:*").Result()
	if err != nil {
		return nil, fmt.Errorf("getting keys: %w", err)
	}

	stats := make(map[string]int64)
	for _, key := range keys {
		len, err := s.client.LLen(ctx, key).Result()
		if err != nil {
			continue
		}
		queryRules := key[5:]
		stats[queryRules] = len
	}

	return stats, nil
}
