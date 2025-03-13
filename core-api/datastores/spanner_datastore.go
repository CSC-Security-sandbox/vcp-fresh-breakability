package datastores

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/spanner"
	_ "github.com/googleapis/go-sql-spanner"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/datamodel"
	"gorm.io/gorm"

	spannergorm "github.com/googleapis/go-gorm-spanner"
)

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ Datastore = (*SpannerDatastore)(nil)

type SpannerDatastore struct {
	dataClient *spanner.Client
	db         *gorm.DB
}

func NewSpannerDatastore(projectID, instanceID, databaseID string) *SpannerDatastore {
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID),
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		log.Fatal(err)
	}

	tables := []interface{}{
		&datamodel.Pool{},
	}

	// Unwrap the underlying SpannerMigrator interface. This interface supports
	// the `AutoMigrateDryRun` method, which does not actually execute the
	// generated statements, and instead just returns these as an array.
	m := db.Migrator()
	migrator, ok := m.(spannergorm.SpannerMigrator)
	if !ok {
		fmt.Printf("unexpected migrator type: %v\n", m)
		return nil
	}
	// Dry-run the migrations and print the generated statements.
	statements, err := migrator.AutoMigrateDryRun(tables...)
	if err != nil {
		fmt.Printf("could not dry-run migrations: %v", err)
		return nil
	}
	fmt.Print("\nMigrations dry-run generated these statements:\n\n")
	for _, statement := range statements {
		fmt.Printf("%s;\n", statement.SQL)
	}

	// Run the same migration for real if you are content with the
	// outcome of the dry run.
	if err := migrator.AutoMigrate(tables...); err != nil {
		fmt.Printf("could not execute migrations: %v", err)
		return nil
	}
	fmt.Println("Executed migrations on Spanner")

	dataClient, err := spanner.NewClient(context.Background(), fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID))
	return &SpannerDatastore{
		dataClient: dataClient,
		db:         db,
	}

}

func (s SpannerDatastore) GetPool(uuid string) (*datamodel.Pool, error) {
	var pool datamodel.Pool
	if err := s.db.First(&pool, s.db.Where("id = ?", uuid)).Error; err != nil {
		log.Fatal(err)
	}
	return &pool, nil
}

func (s SpannerDatastore) CreatePool(pool datamodel.Pool) error {
	s.db.Save(&pool)
	return nil
}

func (s SpannerDatastore) UpdatePool(pool datamodel.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) DeletePool(uuid string) error {
	//TODO implement me
	panic("implement me")
}
