package gorm

import (
	"context"
	"database/sql"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Wrapper struct {
	db *gorm.DB
}

func New(db *gorm.DB) *Wrapper {
	return &Wrapper{db: db}
}

func (w *Wrapper) GORM() *gorm.DB {
	return w.db
}

func (w *Wrapper) Commit() error {
	return w.db.Commit().Error
}

func (w *Wrapper) Rollback() error {
	return w.db.Rollback().Error
}

func (w *Wrapper) DB() (*sql.DB, error) {
	return w.db.DB()
}

func (w *Wrapper) Begin() *Wrapper {
	return &Wrapper{db: w.db.Begin()}
}

func (w *Wrapper) AutoMigrate(values ...interface{}) error {
	return w.db.AutoMigrate(values...)
}

func (w *Wrapper) Set(name string, value interface{}) *Wrapper {
	return &Wrapper{db: w.db.Set(name, value)}
}

func (w *Wrapper) Raw(sql string, values ...interface{}) *Wrapper {
	return &Wrapper{db: w.db.Raw(sql, values...)}
}

func (w *Wrapper) Exec(sql string, values ...interface{}) *Wrapper {
	return &Wrapper{db: w.db.Exec(sql, values...)}
}

func (w *Wrapper) Scan(dest interface{}) *Wrapper {
	return &Wrapper{db: w.db.Scan(dest)}
}

func (w *Wrapper) Error() error {
	return w.db.Error
}

func (w *Wrapper) Save(value interface{}) *Wrapper {
	return &Wrapper{db: w.db.Omit(clause.Associations).Save(value)}
}

func (w *Wrapper) First(dest interface{}, conds ...interface{}) *Wrapper {
	return &Wrapper{db: w.db.First(dest, conds...)}
}

func (w *Wrapper) Where(query interface{}, args ...interface{}) *Wrapper {
	return &Wrapper{db: w.db.Where(query, args...)}
}

func (w *Wrapper) WithContext(ctx context.Context) *Wrapper {
	return &Wrapper{db: w.db.WithContext(ctx)}
}
