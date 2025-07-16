package utils

import (
	"time"

	"gorm.io/gorm"
)

type Transaction interface {
	GORM() *gorm.DB
	Commit() error
	Rollback() error
}

type DbConfig struct {
	Type              string
	Host              string
	Port              string
	User              string
	Password          string
	Name              string
	SSLMode           string
	TimeZone          string
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	ConnectionTimeout int
	AdminUser         string
	AdminPassword     string
}
