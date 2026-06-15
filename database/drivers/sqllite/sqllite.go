package sqllite

import (
	"context"
	"fmt"
	"strings"

	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type Migrator struct {
	Logger log.Logger
	Models []interface{}
}

func (m *Migrator) Migrate(db *gormwrapper.Wrapper, ctx context.Context) error {
	m.Logger.InfoContext(ctx, "Running AutoMigrate for model changes")
	if err := db.WithContext(ctx).AutoMigrate(m.Models...); err != nil {
		return fmt.Errorf("automigrate failed: %w", err)
	}
	return nil
}

func (m *Migrator) Rollback(_ *gormwrapper.Wrapper, _ context.Context) error {
	return nil
}

// CreateOrUpdateViews ensures all required views are created or updated after migrations.
func (m *Migrator) CreateOrUpdateViews(db *gormwrapper.Wrapper) error {
	return CreateOrUpdateViews(db)
}

func CreateOrUpdateViews(db *gormwrapper.Wrapper) error {
	if err := CreateOrUpdatePoolView(db); err != nil {
		return err
	}
	// Add more view creation functions here as needed, e.g.:
	// if err := CreateOrUpdateVolumeView(db); err != nil {
	//     return err
	// }
	return nil
}

// CreateOrUpdatePoolView ensures the pool_view is always in sync with the pool table schema.
func CreateOrUpdatePoolView(db *gormwrapper.Wrapper) error {
	const viewSQL = `CREATE VIEW pool_views AS
	SELECT
		p.*,
		coalesce(
			CASE
				WHEN p.qos_type = 'manual' AND v.volume_performance_group_id IS NOT NULL AND vpg.allocation_type = 'PER_VOLUME'
					THEN sum(vpg.throughput_mibps)
				WHEN p.qos_type = 'manual' AND v.volume_performance_group_id IS NOT NULL AND vpg.allocation_type = 'SHARED'
					THEN sum(vpg.throughput_mibps) / NULLIF((
						SELECT COUNT(*) FROM volumes v2 WHERE v2.volume_performance_group_id = vpg.id AND v2.deleted_at IS NULL), 0)
				ELSE sum(v.throughput)
			END,
			0.0
		) as throughput,
		coalesce(
			CASE
				WHEN p.qos_type = 'manual' AND v.volume_performance_group_id IS NOT NULL AND vpg.allocation_type = 'PER_VOLUME'
					THEN sum(vpg.iops)
				WHEN p.qos_type = 'manual' AND v.volume_performance_group_id IS NOT NULL AND vpg.allocation_type = 'SHARED'
					THEN sum(vpg.iops) / NULLIF((
						SELECT COUNT(*) FROM volumes v2 WHERE v2.volume_performance_group_id = vpg.id AND v2.deleted_at IS NULL), 0)
				ELSE 0
			END,
			0
		) as iops,
		coalesce(max(0, sum(v.size_in_bytes - v.clones_shared_bytes)), 0) as quota_in_bytes,
		coalesce(sum(CASE WHEN v.clones_shared_bytes > 0 THEN 1 ELSE 0 END), 0) as thin_clone_volume_count,
		count(v.id) as volume_count
	FROM pools p
		LEFT JOIN volumes v on v.pool_id = p.id
		and v.account_id = p.account_id
		and v.deleted_at is null
		LEFT JOIN volume_performance_groups vpg on vpg.id = v.volume_performance_group_id
	GROUP BY
		p.id,
		p.name;`

	err := db.Exec(viewSQL).Error()
	if err == nil {
		return nil
	}
	// SQLSTATE 42P16: column order/type mismatch, drop and recreate
	if strings.Contains(err.Error(), "42P16") || strings.Contains(err.Error(), "already exists") {
		dropErr := db.Exec("DROP VIEW IF EXISTS pool_views;").Error()
		if dropErr != nil {
			return dropErr
		}
		return db.Exec(viewSQL).Error()
	}
	return err
}
