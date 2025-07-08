package repository

import (
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/gorm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/sqllite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func SetupTestDB() (*gorm.DB, error) {
	// This function sets up an in-memory SQLite database for testing purposes.
	return SetupSqliteTestDB("")
}

func SetupTestFileDB() (*gorm.DB, string, error) {
	dbname := "/tmp/testdb-" + utils.GenerateRandomAlphanumeric(16) + ".sqlite"

	// Append journal_mode=WAL and synchronous=NORMAL for concurrency and durability
	dsn := dbname + "?_journal_mode=WAL&_synchronous=NORMAL&_txlock=immediate&cache=shared"
	db, err := SetupSqliteTestDB(dsn)
	return db, dbname, err
}

func SetupSqliteTestDB(dbname string) (*gorm.DB, error) {
	if dbname == "" {
		dbname = ":memory:"
	}
	db, err := gorm.Open(sqlite.Open(dbname), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// Perform any necessary migrations or setup here
	err = db.AutoMigrate(&datamodel.Pool{},
		&datamodel.Volume{},
		&datamodel.VolumeReplication{},
		&datamodel.Account{},
		&datamodel.HostGroup{},
		&datamodel.Svm{},
		&datamodel.Node{},
		&datamodel.Lif{},
		&datamodel.Job{},
		&datamodel.Snapshot{},
		&datamodel.ServiceAccount{},
		&datamodel.KmsConfig{},
		&datamodel.BackupVault{},
		&datamodel.Backup{},
		&datamodel.AdminJobSpec{},
		&datamodel.BackupPolicy{})
	if err != nil {
		return nil, err
	}
	err = sqllite.CreateOrUpdateViews(gormwrapper.New(db))
	if err != nil {
		return nil, err
	}
	return db, nil
}

// cleanupTestDBFile closes the DB and removes the file.
func cleanupTestDBFile(db *gorm.DB, fileName string) {
	_ = db.Exec("PRAGMA wal_checkpoint; PRAGMA journal_mode=DELETE;")
	dbConn, _ := db.DB()
	_ = dbConn.Close()
	_ = os.Remove(fileName)
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
		&datamodel.AdminJobSpec{},
		&datamodel.Backup{},
	}

	for _, table := range tables {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(table).Error; err != nil {
			return err
		}
	}
	return nil
}
