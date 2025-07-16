package database

import (
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/drivers/sqllite"
	gormwrapper "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils/gorm"
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
	err = db.AutoMigrate(getVcpModels()...)
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
	for _, table := range getVcpModels() {
		if err := db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(table).Error; err != nil {
			return err
		}
	}
	return nil
}
