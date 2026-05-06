package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

func rollbackAll(cfg config) error {
	// Validate required configuration
	if cfg.vcpDBName == "" {
		return fmt.Errorf("DB_NAME (vcp database name) is required for rollback")
	}
	if cfg.iamVcpCore == "" {
		return fmt.Errorf("IAM_VCP_CORE is required for rollback")
	}
	if cfg.iamTemporal == "" && cfg.temporalEnabled {
		return fmt.Errorf("IAM_TEMPORAL is required when TEMPORAL_ENABLED=true")
	}

	if err := rollbackDB(cfg, cfg.vcpDBName, cfg.iamVcpCore, []string{"postgres"}); err != nil {
		return err
	}
	if cfg.metricsEnabled {
		if err := rollbackDB(cfg, cfg.metricsDBName, cfg.iamVcpCore, []string{"postgres", "metrics"}); err != nil {
			return err
		}
	}
	if cfg.temporalEnabled {
		if err := rollbackDB(cfg, cfg.temporalDBName, cfg.iamTemporal, []string{"postgres"}); err != nil {
			return err
		}
		if err := rollbackDB(cfg, cfg.temporalVisDBName, cfg.iamTemporal, []string{"postgres"}); err != nil {
			return err
		}
	}
	slog.Info("rollback complete for all databases")
	return nil
}

func rollbackDB(cfg config, dbName, currentOwner string, grantUsers []string) error {
	slog.Info("rollback: processing", "db", dbName)

	adminDB, err := connectAdmin(cfg, dbName)
	if err != nil {
		return fmt.Errorf("[%s] admin connect: %w", dbName, err)
	}
	defer func() { _ = adminDB.Close() }()

	if isDBInTargetState(adminDB, cfg.adminUser, grantUsers) {
		slog.Info("rollback: ownership + grants + default privs all correct, skipping", "db", dbName)
		return nil
	}

	if needsOwnershipTransfer(adminDB, cfg.adminUser) {
		slog.Info("R1: ownership transfer needed, transferring back to postgres", "db", dbName, "from", currentOwner)
		if err := rollbackOwnershipViaIAM(cfg, adminDB, dbName, currentOwner); err != nil {
			slog.Warn("IAM-based transfer failed, falling back to SET ROLE", "db", dbName, "error", err)
			if err := rollbackOwnershipViaSetRole(adminDB, dbName, currentOwner, cfg.adminUser); err != nil {
				return fmt.Errorf("[%s] rollback ownership transfer: %w", dbName, err)
			}
		}
	} else {
		slog.Info("R1: ownership already with postgres, skipping", "db", dbName)
	}

	var schemaOwner string
	if err := adminDB.QueryRow(`
		SELECT r.rolname FROM pg_namespace n
		JOIN pg_roles r ON n.nspowner = r.oid
		WHERE n.nspname = 'public'`).Scan(&schemaOwner); err != nil {
		slog.Warn("could not check schema owner, will reset anyway", "db", dbName, "error", err)
	} else if schemaOwner == cfg.adminUser {
		slog.Info("R3: schema already owned by admin, skipping ALTER SCHEMA", "db", dbName)
	}
	if schemaOwner != cfg.adminUser {
		slog.Info("R3: resetting schema owner to admin", "db", dbName, "current_owner", schemaOwner)
		if err := execSQL(adminDB, `ALTER SCHEMA public OWNER TO postgres`); err != nil {
			return fmt.Errorf("[%s] schema owner reset: %w", dbName, err)
		}
	}
	nonOwnerUsers := excludeUser(grantUsers, cfg.adminUser)
	if len(nonOwnerUsers) > 0 {
		if err := grantDML(adminDB, nonOwnerUsers); err != nil {
			return fmt.Errorf("[%s] rollback DML grants: %w", dbName, err)
		}
	}
	if err := setDefaultPrivileges(adminDB, cfg.adminUser, grantUsers); err != nil {
		return fmt.Errorf("[%s] rollback default privileges: %w", dbName, err)
	}

	slog.Info("rollback done", "db", dbName)
	return nil
}

func rollbackOwnershipViaIAM(cfg config, adminDB *sql.DB, dbName, currentOwner string) error {
	// Re-ensure postgres role membership in case a previous partial rollback
	// revoked it. Harmless if already granted.
	if err := execSQL(adminDB, fmt.Sprintf(`GRANT postgres TO %s`, qi(currentOwner))); err != nil {
		if !strings.Contains(err.Error(), "already a member") {
			slog.Warn("could not grant postgres role membership, may fail during ownership transfer", "user", currentOwner, "error", err)
		}
	}

	port := iamPort(cfg, currentOwner)
	slog.Info("R1-IAM: connecting as IAM owner", "db", dbName, "user", currentOwner, "port", port)

	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		cfg.dbHost, port, currentOwner, dbName, cfg.dbSSLMode)
	iamDB, err := openWithRetryN(dsn, 5)
	if err != nil {
		return fmt.Errorf("connect as %s: %w", currentOwner, err)
	}
	defer func() { _ = iamDB.Close() }()

	return transferOwnership(iamDB, "postgres")
}

func rollbackOwnershipViaSetRole(adminDB *sql.DB, dbName, currentOwner, adminUser string) error {
	slog.Info("R1-SETROLE: fallback", "db", dbName, "from", currentOwner)

	// Best-effort revoke to break circular dependency - non-critical if it fails
	if err := execSQL(adminDB, fmt.Sprintf(`REVOKE %s FROM %s`, qi(adminUser), qi(currentOwner))); err != nil {
		slog.Warn("could not revoke role (may not exist), continuing", "role", adminUser, "from", currentOwner, "error", err)
	}
	if err := execSQL(adminDB, fmt.Sprintf(`GRANT %s TO %s`, qi(currentOwner), qi(adminUser))); err != nil {
		return fmt.Errorf("[%s] grant %s to admin: %w", dbName, currentOwner, err)
	}
	if err := execSQL(adminDB, fmt.Sprintf(`SET ROLE %s`, qi(currentOwner))); err != nil {
		return fmt.Errorf("[%s] SET ROLE %s: %w", dbName, currentOwner, err)
	}
	if err := transferOwnership(adminDB, "postgres"); err != nil {
		_, _ = adminDB.Exec(`RESET ROLE`)
		return fmt.Errorf("[%s] transfer ownership: %w", dbName, err)
	}
	if _, err := adminDB.Exec(`RESET ROLE`); err != nil {
		return fmt.Errorf("[%s] RESET ROLE: %w", dbName, err)
	}
	return nil
}
