package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Runner struct {
	apiServer          string
	agentQueryRules    []string
	tags               []string
	queue              string
	buildkiteAgentPath string
	buildkiteToken     string
	pollInterval       time.Duration
	httpClient         *http.Client
	workerID           string
	logger             zerolog.Logger
}

func NewRunner(apiServer string, agentQueryRules, tags []string, queue, buildkiteAgentPath, buildkiteToken string, pollInterval time.Duration, workerID string, logger zerolog.Logger) *Runner {
	return &Runner{
		apiServer:          apiServer,
		agentQueryRules:    agentQueryRules,
		tags:               tags,
		queue:              queue,
		buildkiteAgentPath: buildkiteAgentPath,
		buildkiteToken:     buildkiteToken,
		pollInterval:       pollInterval,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		workerID: workerID,
		logger:   logger,
	}
}

func (r *Runner) Start(ctx context.Context) error {
	r.logger.Info().Strs("query_rules", r.agentQueryRules).Msg("Starting worker")
	r.logger.Info().Dur("poll_interval", r.pollInterval).Msg("Poll interval")

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info().Msg("Worker shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := r.processNextJob(ctx); err != nil {
				if err != ErrNoJobAvailable {
					r.logger.Error().Err(err).Msg("Error processing job")
				}
			}
		}
	}
}

var ErrNoJobAvailable = fmt.Errorf("no job available")

func (r *Runner) processNextJob(ctx context.Context) error {
	job, err := r.getJob(ctx)
	if err != nil {
		return err
	}
	if job == nil {
		return ErrNoJobAvailable
	}

	r.logger.Info().Str("uuid", job.UUID).Str("queue", job.QueueKey).Strs("rules", job.AgentQueryRules).Msg("Claimed job")

	if err := r.runAgent(ctx, job.UUID); err != nil {
		r.logger.Error().Err(err).Str("uuid", job.UUID).Msg("Error running agent")
		return err
	}

	if err := r.completeJob(ctx, job.UUID); err != nil {
		r.logger.Error().Err(err).Str("uuid", job.UUID).Msg("Error marking job complete")
	}

	r.logger.Info().Str("uuid", job.UUID).Msg("Completed job")
	return nil
}

func (r *Runner) getJob(ctx context.Context) (*types.Job, error) {
	queryRules := r.agentQueryRules
	if r.queue != "" {
		queryRules = append([]string{fmt.Sprintf("queue=%s", r.queue)}, queryRules...)
	}
	queryParam := types.NormalizeQueryRules(queryRules)
	url := fmt.Sprintf("%s/jobs?query=%s", r.apiServer, queryParam)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Worker-ID", r.workerID)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var job types.Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("decoding job: %w", err)
	}

	return &job, nil
}

func (r *Runner) runAgent(ctx context.Context, jobUUID string) error {
	allTags := make([]string, 0, len(r.agentQueryRules)+len(r.tags))
	allTags = append(allTags, r.agentQueryRules...)
	allTags = append(allTags, r.tags...)

	tagsValue := r.normalizeTags(allTags)

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	args := []string{
		"start",
		"--acquire-job", jobUUID,
		"--token", r.buildkiteToken,
		"--tags", tagsValue,
		"--name", fmt.Sprintf("worker-%s", hostname),
	}

	if r.queue != "" {
		args = append(args, "--queue", r.queue)
	}

	cmd := exec.CommandContext(ctx, r.buildkiteAgentPath, args...)

	cmd.Stdout = &prefixedWriter{prefix: fmt.Sprintf("[%s] ", jobUUID[:8])}
	cmd.Stderr = &prefixedWriter{prefix: fmt.Sprintf("[%s] ", jobUUID[:8])}

	r.logger.Info().Str("job_uuid", jobUUID).Str("tags", tagsValue).Str("queue", r.queue).Str("name", hostname).Msg("Starting agent")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("running buildkite-agent: %w", err)
	}

	return nil
}

// normalizeTags combines tags into a comma-separated string. For the "queue" key,
// the last value wins to allow later sources (e.g., WORKER_TAGS) to override earlier
// sources (e.g., WORKER_AGENT_QUERY_RULES). All other tags are passed through as-is,
// allowing duplicates.
//
// Example: ["queue=default", "arch=amd64", "queue=production"] -> "arch=amd64,queue=production"
func (r *Runner) normalizeTags(tags []string) string {
	result := []string{}
	lastQueue := ""

	for _, tag := range tags {
		parts := strings.SplitN(tag, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		value := parts[1]

		if key == "queue" {
			lastQueue = value
		} else {
			result = append(result, tag)
		}
	}

	if lastQueue != "" {
		result = append(result, fmt.Sprintf("queue=%s", lastQueue))
	}

	return strings.Join(result, ",")
}

func (r *Runner) completeJob(ctx context.Context, jobUUID string) error {
	url := fmt.Sprintf("%s/jobs/%s/complete", r.apiServer, jobUUID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("X-Worker-ID", r.workerID)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("completing job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

type prefixedWriter struct {
	prefix string
}

func (w *prefixedWriter) Write(p []byte) (n int, err error) {
	lines := strings.Split(string(p), "\n")
	for _, line := range lines {
		if line != "" {
			log.Info().Str("prefix", w.prefix).Msg(line)
		}
	}
	return len(p), nil
}
