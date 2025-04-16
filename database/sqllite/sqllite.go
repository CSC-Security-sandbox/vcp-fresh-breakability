package sqllite

import (
	"context"
	"fmt"

	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type Migrator struct {
	Logger log.Logger
	Models []interface{}
}

func (m *Migrator) Migrate(db *gormwrapper.Wrapper, ctx context.Context) error {
	m.Logger.Info(ctx, "Running AutoMigrate for model changes")
	if err := db.WithContext(ctx).AutoMigrate(m.Models...); err != nil {
		return fmt.Errorf("automigrate failed: %w", err)
	}
	return nil
}

func (m *Migrator) Rollback(_ *gormwrapper.Wrapper, _ context.Context) error {
	return nil
}
