package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/buildkite/buildkite-custom-scheduler/internal/storage"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

type API struct {
	store  *storage.RedisStore
	logger *zerolog.Logger
}

func NewAPI(store *storage.RedisStore, logger *zerolog.Logger) *API {
	return &API{store: store, logger: logger}
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /jobs", a.handleGetJob)
	mux.HandleFunc("POST /jobs/{uuid}/complete", a.handleCompleteJob)
	mux.HandleFunc("GET /stats", a.handleStats)
	mux.ServeHTTP(w, r)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (a *API) handleGetJob(w http.ResponseWriter, r *http.Request) {
	queryParam := r.URL.Query().Get("query")
	if queryParam == "" {
		http.Error(w, "query parameter is required", http.StatusBadRequest)
		return
	}

	queryRules := strings.Split(queryParam, ",")
	for i := range queryRules {
		queryRules[i] = strings.TrimSpace(queryRules[i])
	}

	workerID := r.Header.Get("X-Worker-ID")
	hlog.FromRequest(r).Debug().
		Strs("query_rules", queryRules).
		Str("worker_id", workerID).
		Msg("claiming job")

	job, err := a.store.ClaimJob(r.Context(), queryRules)
	if err != nil {
		a.logger.Error().Err(err).Msg("Error claiming job")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if job == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (a *API) handleCompleteJob(w http.ResponseWriter, r *http.Request) {
	uuid := r.PathValue("uuid")
	if uuid == "" {
		http.Error(w, "job uuid is required", http.StatusBadRequest)
		return
	}

	if err := a.store.CompleteJob(r.Context(), uuid); err != nil {
		a.logger.Error().Err(err).Str("uuid", uuid).Msg("Error completing job")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (a *API) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := a.store.GetAllStats(r.Context())
	if err != nil {
		a.logger.Error().Err(err).Msg("Error getting stats")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	response := make(map[string]interface{})
	response["queues"] = stats

	total := int64(0)
	for _, count := range stats {
		total += count
	}
	response["total"] = total

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /jobs", a.handleGetJob)
	mux.HandleFunc("POST /jobs/{uuid}/complete", a.handleCompleteJob)
	mux.HandleFunc("GET /stats", a.handleStats)

	handler := hlog.RequestIDHandler("request_id", "Request-Id")(mux)
	handler = hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		hlog.FromRequest(r).Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", status).
			Int("size", size).
			Dur("duration", duration).
			Msg("request")
	})(handler)
	handler = hlog.NewHandler(*a.logger)(handler)

	return handler
}
