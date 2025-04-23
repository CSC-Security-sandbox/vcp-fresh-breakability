package repository

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

func SetupTestDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Perform any necessary migrations or setup here
	err = db.AutoMigrate(&datamodel.Pool{}, &datamodel.Volume{}, &datamodel.Account{})
	if err != nil {
		return nil, err
	}

	return db, nil
}

// ClearInMemoryDB deletes all data from the in-memory database.
func ClearInMemoryDB(db *gorm.DB) error {
	tables := []interface{}{
		&datamodel.Pool{},
		&datamodel.Volume{},
		&datamodel.Account{},
	}

	for _, table := range tables {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(table).Error; err != nil {
			return err
		}
	}
	return nil
}
