package main

import (
	"os"
	"strings"
)

func qi(id string) string {
	escaped := strings.ReplaceAll(id, `"`, `""`)
	return `"` + escaped + `"`
}

func qs(val string) string {
	escaped := strings.ReplaceAll(val, `'`, `''`)
	return `'` + escaped + `'`
}

func joinQI(ids []string) string {
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = qi(id)
	}
	return strings.Join(q, ", ")
}

func allIAMUsers(cfg config) []string {
	return []string{cfg.iamVcpCore, cfg.iamVcpWorker, cfg.iamClhSA, cfg.iamTemporal, cfg.iamMetricsProducer}
}

func roleMembershipUsers(cfg config) []string {
	return []string{cfg.iamVcpCore, cfg.iamVcpWorker, cfg.iamClhSA, cfg.iamTemporal}
}

func vcpGrantUsers(cfg config) []string {
	return []string{cfg.iamVcpCore, cfg.iamVcpWorker, cfg.iamClhSA, cfg.iamMetricsProducer, "postgres"}
}

func metricsGrantUsers(cfg config) []string {
	return []string{cfg.iamVcpCore, cfg.iamVcpWorker, cfg.iamClhSA, cfg.iamMetricsProducer, "postgres", "metrics"}
}

func temporalGrantUsers(cfg config) []string {
	return []string{cfg.iamTemporal, "postgres"}
}

func excludeUser(users []string, exclude string) []string {
	var out []string
	for _, u := range users {
		if u != exclude {
			out = append(out, u)
		}
	}
	return out
}

func iamPort(cfg config, iamUser string) string {
	if cfg.temporalDBPort != "" && iamUser == cfg.iamTemporal {
		return cfg.temporalDBPort
	}
	return cfg.dbPort
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string) bool {
	v := os.Getenv(key)
	return v == "true" || v == "1"
}
