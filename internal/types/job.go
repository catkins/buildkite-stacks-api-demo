package types

import (
	"sort"
	"strings"
	"time"
)

type Job struct {
	UUID            string    `json:"uuid"`
	QueueKey        string    `json:"queue_key"`
	AgentQueryRules []string  `json:"agent_query_rules"`
	Priority        int       `json:"priority"`
	ScheduledAt     time.Time `json:"scheduled_at"`
	ReservedAt      time.Time `json:"reserved_at"`
}

func NormalizeQueryRules(rules []string) string {
	if len(rules) == 0 {
		return ""
	}
	sorted := make([]string, len(rules))
	copy(sorted, rules)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

func ParseQueryRules(normalized string) []string {
	if normalized == "" {
		return []string{}
	}
	return strings.Split(normalized, ",")
}
