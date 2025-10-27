package server

import (
	"context"
	"fmt"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/storage"
	"github.com/buildkite/buildkite-custom-scheduler/internal/types"
	"github.com/buildkite/stacksapi"
	"github.com/rs/zerolog/log"
)

type Monitor struct {
	client   *stacksapi.Client
	stackKey string
	queues   []string
	store    *storage.RedisStore
	interval time.Duration
}

func NewMonitor(client *stacksapi.Client, stackKey string, queues []string, store *storage.RedisStore, interval time.Duration) *Monitor {
	return &Monitor{
		client:   client,
		stackKey: stackKey,
		queues:   queues,
		store:    store,
		interval: interval,
	}
}

func (m *Monitor) Start(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	log.Info().Strs("queues", m.queues).Dur("interval", m.interval).Msg("Starting monitor")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Monitor shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := m.pollQueues(ctx); err != nil {
				log.Error().Err(err).Msg("Error polling queues")
			}
		}
	}
}

func (m *Monitor) pollQueues(ctx context.Context) error {
	for _, queueKey := range m.queues {
		if err := m.pollQueue(ctx, queueKey); err != nil {
			log.Error().Err(err).Str("queue", queueKey).Msg("Error polling queue")
		}
	}
	return nil
}

func (m *Monitor) pollQueue(ctx context.Context, queueKey string) error {
	var cursor string
	jobsProcessed := 0

	for {
		resp, _, err := m.client.ListScheduledJobs(ctx, stacksapi.ListScheduledJobsRequest{
			StackKey:        m.stackKey,
			ClusterQueueKey: queueKey,
			PageSize:        50,
			StartCursor:     cursor,
		})
		if err != nil {
			return fmt.Errorf("listing scheduled jobs: %w", err)
		}

		if resp.ClusterQueue.Paused {
			log.Info().Str("queue", queueKey).Msg("Queue is paused, skipping")
			return nil
		}

		if len(resp.Jobs) > 0 {
			if err := m.reserveJobs(ctx, queueKey, resp.Jobs); err != nil {
				log.Error().Err(err).Msg("Error reserving jobs")
			} else {
				jobsProcessed += len(resp.Jobs)
			}
		}

		if !resp.PageInfo.HasNextPage {
			break
		}
		cursor = resp.PageInfo.EndCursor
	}

	if jobsProcessed > 0 {
		log.Info().Int("count", jobsProcessed).Str("queue", queueKey).Msg("Processed jobs")
	}

	return nil
}

func (m *Monitor) reserveJobs(ctx context.Context, queueKey string, jobs []stacksapi.ScheduledJob) error {
	if len(jobs) == 0 {
		return nil
	}

	jobUUIDs := make([]string, len(jobs))
	for i, job := range jobs {
		jobUUIDs[i] = job.ID
	}

	reserved, _, err := m.client.BatchReserveJobs(ctx, stacksapi.BatchReserveJobsRequest{
		StackKey:                 m.stackKey,
		JobUUIDs:                 jobUUIDs,
		ReservationExpirySeconds: 300,
	})
	if err != nil {
		return fmt.Errorf("batch reserve jobs: %w", err)
	}

	reservedMap := make(map[string]bool)
	for _, uuid := range reserved.Reserved {
		reservedMap[uuid] = true
	}

	for _, job := range jobs {
		if !reservedMap[job.ID] {
			continue
		}

		ourJob := &types.Job{
			UUID:            job.ID,
			QueueKey:        queueKey,
			AgentQueryRules: job.AgentQueryRules,
			Priority:        job.Priority,
			ScheduledAt:     job.ScheduledAt,
			ReservedAt:      time.Now(),
		}

		if err := m.store.AddJob(ctx, ourJob); err != nil {
			log.Error().Err(err).Str("job_id", job.ID).Msg("Error storing job")
		}
	}

	log.Info().Int("reserved", len(reserved.Reserved)).Int("total", len(jobs)).Str("queue", queueKey).Msg("Reserved jobs")

	return nil
}
