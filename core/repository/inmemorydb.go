package repository

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func SetupTestDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Perform any necessary migrations or setup here
	err = db.AutoMigrate(&datamodel.Pool{}, &datamodel.Volume{}, &datamodel.VolumeReplication{}, &datamodel.Account{}, &datamodel.HostGroup{}, &datamodel.Svm{}, &datamodel.Node{}, &datamodel.Lif{}, &datamodel.Job{}, &datamodel.Snapshot{}, &datamodel.ServiceAccount{}, &datamodel.KmsConfig{}, &datamodel.BackupVault{})
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
		&datamodel.HostGroup{},
		&datamodel.Account{},
		&datamodel.Svm{},
		&datamodel.HostGroup{},
		&datamodel.VolumeReplication{},
		&datamodel.Node{},
		&datamodel.Svm{},
		&datamodel.Lif{},
		&datamodel.Job{},
		&datamodel.KmsConfig{},
		&datamodel.ServiceAccount{},
		&datamodel.BackupVault{},
	}

	for _, table := range tables {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(table).Error; err != nil {
			return err
		}
	}
	return nil
}
