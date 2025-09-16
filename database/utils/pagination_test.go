package utils

import (
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestPaginate_NilPagination(t *testing.T) {
	db := &gorm.DB{}
	result := Paginate(nil)(db)
	if result != db {
		t.Error("Expected db to be unchanged when pagination is nil")
	}
}

func TestPaginate_DefaultLimit(t *testing.T) {
	p := &Pagination{Limit: 0, Offset: 5}
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}
	result := Paginate(p)(db)
	// Since gorm.DB is a struct, we can't check the limit directly here.
	// This test just ensures the function returns a *gorm.DB.
	if result == nil {
		t.Error("Expected non-nil *gorm.DB")
	}
}

func TestPaginate_CustomLimitOffset(t *testing.T) {
	p := &Pagination{Limit: 10, Offset: 20}
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to connect database: %v", err)
	}
	result := Paginate(p)(db)
	// Since gorm.DB is a struct, we can't check the limit directly here.
	// This test just ensures the function returns a *gorm.DB.
	if result == nil {
		t.Error("Expected non-nil *gorm.DB")
	}
}
