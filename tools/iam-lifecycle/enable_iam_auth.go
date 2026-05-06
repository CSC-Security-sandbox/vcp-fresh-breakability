package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
)

func enableAll(cfg config) error {
	// Validate required configuration
	if cfg.vcpDBName == "" {
		return fmt.Errorf("DB_NAME (vcp database name) is required")
	}
	if cfg.iamVcpCore == "" {
		return fmt.Errorf("IAM_VCP_CORE is required")
	}
	if cfg.iamVcpWorker == "" {
		return fmt.Errorf("IAM_VCP_WORKER is required")
	}
	if cfg.iamTemporal == "" && cfg.temporalEnabled {
		return fmt.Errorf("IAM_TEMPORAL is required when TEMPORAL_ENABLED=true")
	}
	if cfg.metricsDBName == "" && cfg.metricsEnabled {
		return fmt.Errorf("METRICS_DB_NAME is required when METRICS_ENABLED=true")
	}

	if err := enableDB(cfg, cfg.vcpDBName, cfg.iamVcpCore, cfg.iamVcpCore, vcpGrantUsers(cfg)); err != nil {
		return err
	}

	// Grant CREATEDB to temporal-ksa if not already present.
	// Temporal's schema management requires this privilege.
	db, err := connectAdmin(cfg, cfg.vcpDBName)
	if err != nil {
		return fmt.Errorf("connect for CREATEDB grant: %w", err)
	}
	defer func() { _ = db.Close() }()
	var hasCreateDB bool
	if err := db.QueryRow(`SELECT rolcreatedb FROM pg_roles WHERE rolname = $1`, cfg.iamTemporal).Scan(&hasCreateDB); err != nil {
		slog.Warn("could not check CREATEDB, will grant anyway", "user", cfg.iamTemporal, "error", err)
	}
	if hasCreateDB {
		slog.Info("CREATEDB already granted, skipping", "user", cfg.iamTemporal)
	} else {
		slog.Info("granting CREATEDB to temporal-ksa", "user", cfg.iamTemporal)
		if err := execSQL(db, fmt.Sprintf(`ALTER ROLE %s CREATEDB`, qi(cfg.iamTemporal))); err != nil {
			return fmt.Errorf("grant CREATEDB to temporal role: %w", err)
		}
	}

	if cfg.metricsEnabled {
		if err := enableDB(cfg, cfg.metricsDBName, cfg.iamVcpCore, cfg.iamVcpCore, metricsGrantUsers(cfg)); err != nil {
			return err
		}
	}

	if cfg.temporalEnabled {
		if err := enableDB(cfg, cfg.temporalDBName, cfg.iamTemporal, cfg.iamTemporal, temporalGrantUsers(cfg)); err != nil {
			return err
		}
		if err := enableDB(cfg, cfg.temporalVisDBName, cfg.iamTemporal, cfg.iamTemporal, temporalGrantUsers(cfg)); err != nil {
			return err
		}
	}

	slog.Info("enable complete for all databases")
	return nil
}

func enableDB(cfg config, dbName, targetOwner, dmlUser string, grantUsers []string) error {
	slog.Info("enable: processing", "db", dbName)

	adminDB, err := connectAdmin(cfg, dbName)
	if err != nil {
		return fmt.Errorf("[%s] admin connect: %w", dbName, err)
	}
	defer func() { _ = adminDB.Close() }()

	if isDBInTargetState(adminDB, targetOwner, grantUsers) {
		slog.Info("enable: ownership + grants + default privs all correct, skipping", "db", dbName)
		return nil
	}

	if needsOwnershipTransfer(adminDB, targetOwner) {
		slog.Info("phase 1A: ownership transfer needed", "db", dbName, "to", targetOwner)
		if err := transferOwnership(adminDB, targetOwner); err != nil {
			return fmt.Errorf("[%s] ownership transfer: %w", dbName, err)
		}
	} else {
		slog.Info("phase 1A: ownership already correct, skipping", "db", dbName, "owner", targetOwner)
	}

	if err := grantRoleMemberships(adminDB, roleMembershipUsers(cfg)); err != nil {
		return fmt.Errorf("[%s] role membership grant: %w", dbName, err)
	}
	if err := grantSchemaUsage(adminDB, allIAMUsers(cfg)); err != nil {
		return fmt.Errorf("[%s] schema usage grant: %w", dbName, err)
	}

	// DML grants must be issued by the table owner, so we connect as the IAM owner
	// through the appropriate proxy sidecar.
	port := iamPort(cfg, dmlUser)
	slog.Info("phase 1B/1C: DML grants", "db", dbName, "as", dmlUser, "port", port)
	ownerDB, err := connectIAM(cfg, dbName, dmlUser, port)
	if err != nil {
		return fmt.Errorf("[%s] connect as %s: %w", dbName, dmlUser, err)
	}
	defer func() { _ = ownerDB.Close() }()

	nonOwnerUsers := excludeUser(grantUsers, dmlUser)
	if len(nonOwnerUsers) > 0 {
		if err := grantDML(ownerDB, nonOwnerUsers); err != nil {
			return fmt.Errorf("[%s] DML grants: %w", dbName, err)
		}
	}
	if err := setDefaultPrivileges(ownerDB, dmlUser, grantUsers); err != nil {
		return fmt.Errorf("[%s] default privileges: %w", dbName, err)
	}

	slog.Info("enable done", "db", dbName)
	return nil
}

func isDBInTargetState(db *sql.DB, targetOwner string, grantUsers []string) bool {
	var schemaOK int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_namespace n
		JOIN pg_roles r ON n.nspowner = r.oid
		WHERE n.nspname = 'public' AND r.rolname = $1`, targetOwner,
	).Scan(&schemaOK); err != nil || schemaOK == 0 {
		slog.Info("state check: schema not owned by target", "target", targetOwner)
		return false
	}

	var wrongOwners int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_tables
		WHERE schemaname = 'public' AND tableowner != $1`, targetOwner,
	).Scan(&wrongOwners); err != nil || wrongOwners > 0 {
		slog.Info("state check: tables not owned by target", "target", targetOwner, "count", wrongOwners)
		return false
	}

	var tableCount int
	if err := db.QueryRow(
		`SELECT count(*) FROM pg_tables WHERE schemaname = 'public'`,
	).Scan(&tableCount); err != nil {
		return false
	}
	if tableCount == 0 {
		return true
	}

	// Verify every non-owner grant user has SELECT on all tables.
	for _, u := range grantUsers {
		if u == targetOwner {
			continue
		}
		var missing int
		if err := db.QueryRow(`
			SELECT count(*) FROM pg_tables t
			WHERE t.schemaname = 'public'
			AND NOT has_table_privilege($1,
				format('%I.%I', t.schemaname, t.tablename), 'SELECT')`, u,
		).Scan(&missing); err != nil || missing > 0 {
			slog.Info("state check: grant user missing DML", "user", u, "missing_tables", missing)
			return false
		}
	}

	// Verify default privileges exist for the target owner (tables + sequences).
	var defAclCount int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_default_acl da
		JOIN pg_namespace n ON da.defaclnamespace = n.oid
		JOIN pg_roles r ON da.defaclrole = r.oid
		WHERE n.nspname = 'public' AND r.rolname = $1`, targetOwner,
	).Scan(&defAclCount); err != nil || defAclCount == 0 {
		slog.Info("state check: default privileges missing", "target", targetOwner)
		return false
	}

	return true
}

func needsOwnershipTransfer(db *sql.DB, targetOwner string) bool {
	var count int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_tables
		WHERE schemaname = 'public' AND tableowner != $1`, targetOwner,
	).Scan(&count); err != nil {
		return true
	}
	return count > 0
}

func transferOwnership(db *sql.DB, target string) error {
	if err := execSQL(db, fmt.Sprintf(`DO $$ DECLARE r text; BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname='public' AND tableowner != %s LOOP
    BEGIN EXECUTE format('ALTER TABLE public.%%I OWNER TO %s', r);
    EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'skip table %%', r; END;
  END LOOP;
END $$`, qs(target), qi(target))); err != nil {
		return fmt.Errorf("transfer table ownership to %s: %w", target, err)
	}

	if err := execSQL(db, fmt.Sprintf(`DO $$ DECLARE r text; BEGIN
  FOR r IN SELECT sequencename FROM pg_sequences WHERE schemaname='public' AND sequenceowner != %s LOOP
    BEGIN EXECUTE format('ALTER SEQUENCE public.%%I OWNER TO %s', r);
    EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'skip seq %%', r; END;
  END LOOP;
END $$`, qs(target), qi(target))); err != nil {
		return fmt.Errorf("transfer sequence ownership to %s: %w", target, err)
	}
	return nil
}

func grantRoleMemberships(db *sql.DB, users []string) error {
	for _, u := range users {
		var isMember bool
		if err := db.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM pg_auth_members m
				JOIN pg_roles gr ON m.roleid = gr.oid
				JOIN pg_roles mr ON m.member = mr.oid
				WHERE gr.rolname = 'postgres' AND mr.rolname = $1
			)`, u).Scan(&isMember); err != nil {
			slog.Warn("could not check role membership, will grant anyway", "user", u, "error", err)
		} else if isMember {
			slog.Info("role membership already exists, skipping", "user", u)
			continue
		}
		slog.Info("granting postgres role membership", "user", u)
		if err := execSQL(db, fmt.Sprintf(`GRANT postgres TO %s`, qi(u))); err != nil {
			if strings.Contains(err.Error(), "already a member") {
				slog.Info("role membership already exists (confirmed by server)", "user", u)
				continue
			}
			return fmt.Errorf("grant postgres to %s: %w", u, err)
		}
	}
	return nil
}

func grantSchemaUsage(db *sql.DB, users []string) error {
	var missing []string
	for _, u := range users {
		var has bool
		if err := db.QueryRow(
			`SELECT has_schema_privilege($1, 'public', 'USAGE')`, u,
		).Scan(&has); err != nil {
			slog.Warn("could not check schema privilege, will grant anyway", "user", u, "error", err)
			missing = append(missing, u)
			continue
		}
		if has {
			slog.Info("schema USAGE already granted, skipping", "user", u)
		} else {
			missing = append(missing, u)
		}
	}
	if len(missing) == 0 {
		slog.Info("schema USAGE already granted to all users, skipping")
		return nil
	}
	slog.Info("granting schema USAGE", "users", missing)
	return execSQL(db, fmt.Sprintf(`GRANT USAGE ON SCHEMA public TO %s`, joinQI(missing)))
}

func grantDML(db *sql.DB, users []string) error {
	var missing []string
	for _, u := range users {
		var count int
		if err := db.QueryRow(`
			SELECT count(*) FROM pg_tables t
			WHERE t.schemaname = 'public'
			AND NOT has_table_privilege($1,
				format('%I.%I', t.schemaname, t.tablename), 'SELECT')`, u,
		).Scan(&count); err != nil {
			slog.Warn("could not check DML privileges, will grant anyway", "user", u, "error", err)
			missing = append(missing, u)
			continue
		}
		if count > 0 {
			slog.Info("user missing DML on tables", "user", u, "missing_tables", count)
			missing = append(missing, u)
		} else {
			slog.Info("DML already granted, skipping", "user", u)
		}
	}
	if len(missing) == 0 {
		slog.Info("DML already granted to all users, skipping")
		return nil
	}
	slog.Info("granting DML", "users", missing)
	list := joinQI(missing)
	if err := execSQL(db, fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s`, list)); err != nil {
		return err
	}
	return execSQL(db, fmt.Sprintf(`GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO %s`, list))
}

func setDefaultPrivileges(db *sql.DB, ownerRole string, users []string) error {
	var defAclCount int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_default_acl da
		JOIN pg_namespace n ON da.defaclnamespace = n.oid
		JOIN pg_roles r ON da.defaclrole = r.oid
		WHERE n.nspname = 'public' AND r.rolname = $1`, ownerRole,
	).Scan(&defAclCount); err != nil {
		slog.Warn("could not check default privileges, will set anyway", "owner", ownerRole, "error", err)
	} else if defAclCount >= 2 {
		slog.Info("default privileges already configured, skipping", "owner", ownerRole, "acl_entries", defAclCount)
		return nil
	} else {
		slog.Info("default privileges incomplete, setting", "owner", ownerRole, "existing_entries", defAclCount)
	}
	list := joinQI(users)
	if err := execSQL(db, fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s`, qi(ownerRole), list)); err != nil {
		return err
	}
	return execSQL(db, fmt.Sprintf(`ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %s`, qi(ownerRole), list))
}
