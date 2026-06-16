package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

func openDB(cfg config) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s connect_timeout=10",
		cfg.dbHost, cfg.dbPort, cfg.dbName, cfg.dbUser, cfg.dbPassword, cfg.dbSSLMode)
	return sql.Open("postgres", dsn)
}

func queryActiveVersions(ctx context.Context, db *sql.DB) ([]string, error) {
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := db.QueryContext(qctx, `
		SELECT DISTINCT build_info->>'ontapVersion'
		FROM pools
		WHERE state <> 'DELETED'
		  AND build_info->>'ontapVersion' IS NOT NULL
		  AND build_info->>'ontapVersion' != ''`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		if v != "" {
			versions = append(versions, v)
		}
	}
	return versions, rows.Err()
}

// runWith is the fully injectable reconciliation core; all infrastructure is passed in.
// Returns an exit code (always 0 — the job exits gracefully even on partial failures).
func runWith(ctx context.Context, cfg config, k8s k8sClientInterface, db *sql.DB, log *slog.Logger) int {
	// ── Query active ONTAP versions ────────────────────────────────────────────

	log.Info("querying active ONTAP versions from database")
	dbStart := time.Now()
	rawVersions, err := queryActiveVersions(ctx, db)
	if err != nil {
		log.Error("database query failed — skipping reconciliation",
			"error", err, "ms", time.Since(dbStart).Milliseconds())
		return 0
	}
	log.Info("DB query complete", "ms", time.Since(dbStart).Milliseconds())

	activeVersions := normalizeVersions(rawVersions)
	vStrs := make([]string, len(activeVersions))
	for i, v := range activeVersions {
		vStrs[i] = v.String()
	}
	log.Info("active versions", "count", len(activeVersions), "versions", strings.Join(vStrs, ", "))

	// G4: Never act on an empty active set.
	if len(activeVersions) == 0 {
		log.Warn("G4: active version set is empty — skipping all scale-down to prevent mass outage")
		return 0
	}

	// ── List vlm-worker Deployments ────────────────────────────────────────────

	log.Info("listing vlm-worker Deployments", "namespace", cfg.namespace)
	deployments, err := k8s.listVLMWorkerDeployments(ctx)
	if err != nil {
		log.Error("failed to list Deployments — skipping reconciliation", "error", err)
		return 0
	}
	if len(deployments) == 0 {
		log.Warn("no vlm-worker Deployments found — nothing to reconcile")
		return 0
	}
	log.Info("found Deployments", "count", len(deployments))

	// ── Classify each Deployment ───────────────────────────────────────────────

	var keepActive, scaleToZero []deploymentItem

	for _, d := range deployments {
		depVer, ok := deployNameToVersion(d.Name)
		if !ok {
			log.Warn("cannot parse version from deployment name — keeping active (safe default)",
				"deployment", d.Name)
			keepActive = append(keepActive, d)
			continue
		}
		keep, reason := shouldKeep(depVer, activeVersions)
		if keep {
			log.Info("KEEP", "deployment", d.Name, "version", depVer.String(), "reason", reason)
			keepActive = append(keepActive, d)
		} else {
			log.Info("SCALE-TO-0", "deployment", d.Name, "version", depVer.String(),
				"reason", "no active version requires this worker")
			scaleToZero = append(scaleToZero, d)
		}
	}

	// G5: Never scale every worker to 0.
	if len(keepActive) == 0 {
		log.Error("G5: hierarchy logic would scale ALL workers to 0 — aborting to prevent full outage")
		return 0
	}

	// ── Apply scale actions ────────────────────────────────────────────────────

	log.Info("result", "keep", len(keepActive), "scale_to_zero", len(scaleToZero))

	if len(scaleToZero) == 0 {
		log.Info("nothing to scale down — all workers are active")
	}

	var scaleFailed int
	for _, d := range scaleToZero {
		if d.Replicas == 0 {
			log.Info("SKIP — already at 0 replicas", "deployment", d.Name)
			continue
		}
		if cfg.dryRun {
			log.Info("DRY-RUN: would scale to 0", "deployment", d.Name, "current_replicas", d.Replicas)
			continue
		}
		log.Info("scaling to 0", "deployment", d.Name, "current_replicas", d.Replicas)
		if err := k8s.scaleDeployment(ctx, d.Name, 0); err != nil {
			log.Error("failed to scale deployment — continuing", "deployment", d.Name, "error", err)
			scaleFailed++
			continue
		}
		log.Info("scaled to 0", "deployment", d.Name)
	}

	if scaleFailed > 0 {
		log.Warn("reconciler complete with partial failures", "scale_failures", scaleFailed)
	} else {
		log.Info("reconciler complete")
	}
	return 0
}

// run builds real infrastructure from cfg and delegates to runWith.
func run(ctx context.Context, cfg config, log *slog.Logger) int {
	log.Info("VLM Worker Reconciler", "namespace", cfg.namespace, "dry_run", cfg.dryRun)

	// Skip gracefully if DB credentials are not available (secret not yet synced).
	if cfg.dbUser == "" || cfg.dbPassword == "" {
		log.Warn("DB credentials missing or secret not synced — skipping reconciliation")
		return 0
	}

	db, err := openDB(cfg)
	if err != nil {
		log.Error("failed to open DB — skipping reconciliation", "error", err)
		return 0
	}
	defer func() { _ = db.Close() }()

	k8s, err := newK8sClient(cfg.namespace)
	if err != nil {
		log.Error("failed to init K8s client — skipping reconciliation", "error", err)
		return 0
	}

	return runWith(ctx, cfg, k8s, db, log)
}
