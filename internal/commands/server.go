package commands

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/server"
	"github.com/buildkite/buildkite-custom-scheduler/internal/storage"
	"github.com/buildkite/stacksapi"
	"github.com/rs/zerolog/log"
)

type ServerCmd struct {
	AgentToken   string   `help:"Buildkite agent token" env:"BUILDKITE_AGENT_TOKEN" required:""`
	StackKey     string   `help:"Unique stack key" default:"custom-scheduler-demo"`
	Queues       []string `help:"Queue keys to monitor" default:"default" env:"SCHEDULER_QUEUES" sep:","`
	RedisAddr    string   `help:"Redis address" default:"localhost:6379" env:"REDIS_ADDR"`
	Listen       string   `help:"HTTP listen address" default:":18888" env:"LISTEN"`
	PollInterval string   `help:"Poll interval" default:"1s" env:"POLL_INTERVAL"`
}

func (s *ServerCmd) Run() error {
	if len(s.Queues) == 0 {
		return fmt.Errorf("at least one queue is required")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().Msg("Starting server...")
	log.Info().Str("stack_key", s.StackKey).Msg("Stack key")
	log.Info().Strs("queues", s.Queues).Msg("Queues")
	log.Info().Str("redis", s.RedisAddr).Msg("Redis")
	log.Info().Str("listen", s.Listen).Msg("Listen")

	store, err := storage.NewRedisStore(s.RedisAddr)
	if err != nil {
		return err
	}
	defer store.Close()
	log.Info().Str("redis", s.RedisAddr).Msg("Connected to Redis")

	client, err := stacksapi.NewClient(s.AgentToken)
	if err != nil {
		return err
	}

	stack, _, err := client.RegisterStack(ctx, stacksapi.RegisterStackRequest{
		Key:      s.StackKey,
		Type:     stacksapi.StackTypeCustom,
		QueueKey: s.Queues[0],
		Metadata: map[string]string{
			"version": "1.0.0",
			"type":    "custom-scheduler-demo",
		},
	})
	if err != nil {
		return err
	}
	log.Info().Str("key", stack.Key).Str("queue", stack.ClusterQueueKey).Msg("Registered stack")

	defer func() {
		log.Info().Str("stack_key", s.StackKey).Msg("Deregistering stack")
		if _, err := client.DeregisterStack(context.Background(), s.StackKey); err != nil {
			log.Error().Err(err).Msg("Failed to deregister stack")
		}
	}()

	pollInterval, err := time.ParseDuration(s.PollInterval)
	if err != nil {
		return err
	}

	monitor := server.NewMonitor(client, s.StackKey, s.Queues, store, pollInterval)
	go func() {
		if err := monitor.Start(ctx); err != nil && err != context.Canceled {
			log.Error().Err(err).Msg("Monitor error")
		}
	}()

	api := server.NewAPI(store, &log.Logger)
	httpServer := &http.Server{
		Addr:    s.Listen,
		Handler: api.Handler(),
	}

	go func() {
		log.Info().Str("listen", s.Listen).Msg("Starting HTTP server")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("HTTP server error")
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Info().Msg("Shutting down gracefully...")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP server shutdown error")
	}

	log.Info().Msg("Shutdown complete")
	return nil
}
