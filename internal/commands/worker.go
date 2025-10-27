package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/worker"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type WorkerCmd struct {
	APIServer       string   `help:"API server URL" default:"http://localhost:18888" env:"WORKER_API_SERVER"`
	AgentQueryRules []string `help:"Agent query rules (defines job matching)" default:"queue=default" env:"WORKER_AGENT_QUERY_RULES" sep:","`
	Tags            []string `help:"Additional agent tags (metadata only, not used for job matching)" env:"WORKER_TAGS" sep:","`
	Queue           string   `help:"Buildkite queue name" default:"" env:"WORKER_QUEUE"`
	AgentPath       string   `help:"Path to buildkite-agent binary" default:"/usr/local/bin/buildkite-agent" env:"BUILDKITE_AGENT_PATH"`
	AgentToken      string   `help:"Buildkite agent token" env:"BUILDKITE_AGENT_TOKEN" required:""`
	PollInterval    string   `help:"Poll interval" default:"2s" env:"WORKER_POLL_INTERVAL"`
}

func (w *WorkerCmd) Run() error {
	if len(w.AgentQueryRules) == 0 {
		return fmt.Errorf("at least one agent query rule is required")
	}

	pollInterval, err := time.ParseDuration(w.PollInterval)
	if err != nil {
		return err
	}

	workerID := uuid.New().String()
	logger := log.With().Str("worker_id", workerID).Logger()

	logger.Info().Msg("Starting worker...")
	logger.Info().Str("api_server", w.APIServer).Msg("API server")
	logger.Info().Strs("query_rules", w.AgentQueryRules).Msg("Query rules")
	logger.Info().Strs("tags", w.Tags).Msg("Additional tags")
	logger.Info().Str("queue", w.Queue).Msg("Queue")
	logger.Info().Str("agent_path", w.AgentPath).Msg("Agent path")
	logger.Info().Dur("poll_interval", pollInterval).Msg("Poll interval")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := worker.NewRunner(
		w.APIServer,
		w.AgentQueryRules,
		w.Tags,
		w.Queue,
		w.AgentPath,
		w.AgentToken,
		pollInterval,
		workerID,
		logger,
	)

	go func() {
		if err := runner.Start(ctx); err != nil && err != context.Canceled {
			logger.Error().Err(err).Msg("Runner error")
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info().Msg("Shutting down gracefully...")
	cancel()

	time.Sleep(2 * time.Second)
	logger.Info().Msg("Shutdown complete")
	return nil
}
