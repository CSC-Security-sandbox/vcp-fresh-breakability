Here's a comprehensive `README.md` for your database implementation:

# Database Abstraction Layer


### Initialization

```go
import (
    "yourproject/database"
    _ "yourproject/database/postgres" // Register PostgreSQL implementation
)

func main() {
    config := database.DbConfig{
        Type:          "postgres",
        Host:          "localhost",
        Port:          "5432",
        User:          "appuser",
        Password:      "apppass",
        Name:          "appdb",
        AdminUser:     "postgres",
        AdminPassword: "postgrespass",
        SSLMode:       "disable",
        MigrationPath: "migrations",
    }

    db, err := database.New(config)
    if err != nil {
        panic(err)
    }
    defer db.Close()
}
```

### CRUD Operations

#### Pool Operations
```go
// Create pool
pool := &datamodel.Pool{Name: "main-pool"}
err := db.CreatePool(context.Background(), pool)

// Get pool
retrievedPool, err := db.GetPool(context.Background(), "pool-id")

// List pools
pools, err := db.ListPools(context.Background())
```

#### Volume Operations
```go
// Create volume
volume := &datamodel.Volume{Name: "data-volume", PoolID: "pool-id"}
err := db.CreateVolume(context.Background(), volume)

// Get volume
retrievedVolume, err := db.GetVolume(context.Background(), "volume-id")

// List volumes by pool
volumes, err := db.ListVolumes(context.Background(), "pool-id")
```

### Transactions
```go
err := db.WithTransaction(context.Background(), func(tx database.Transaction) error {
    // Create pool
    if err := tx.GORM().Create(&datamodel.Pool{Name: "transaction-pool"}).Error; err != nil {
        return err
    }
    
    // Create volume in same pool
    return tx.GORM().Create(&datamodel.Volume{
        Name:   "transaction-volume",
        PoolID: "pool-id",
    }).Error
})
```

### Migrations
```go
// Run migrations


if err := db.Migrate(context.Background(), models); err != nil {
    panic(err)
}


func getModels() []interface{} {

    return []interface{}{
    datamodel.Pool{},
    datamodel.Volume{},
    // add other models here
    }
}
```

## Architecture

```
database/
├── interfaces.go       # Core interfaces
├── factory.go          # Factory registry
├── postgres/
│   ├── storage.go      # PostgreSQL implementation
│   ├── migrator.go     # Migration logic
│   └── migrations/     # SQL migration files
└── gorm/
    └── wrapper.go      # GORM adapter
```

## Extending to New Databases

1. Create new package under `database/` (e.g. `mysql/`)
2. Implement all interface methods
3. Register in `init()`:

```go
package mysql

import "yourproject/database"

func init() {
    database.Register("mysql", NewMySQLStorage)
}

func NewMySQLStorage(config database.DbConfig) (database.Storage, error) {
    // implementation
}
```

## Best Practices

1. **Always use contexts** - Pass contexts for cancellation/timeouts
2. **Use transactions** - For any multi-operation writes
3. **Avoid nested transactions** - For any multi-operations
3. **Check errors** - Always handle database errors
4. **Close connections** - Use defer db.Close() after initialization
5. **Monitor health** - Use HealthCheck() in readiness probes

## Troubleshooting

### "unsupported database type" error
- Ensure you've imported the database implementation:
  ```go
  import _ "yourproject/database/postgres"
  ```
- Verify the type in config matches registered types

### Migration issues
- Check migration files are in correct directory
- Verify database user has sufficient privileges
- Examine SQL logs for syntax errors

## Example Service

#### To illustrate usage of transactions and CRUD operations

```go
package service

type PoolService struct {
    db database.Storage
}

func (s *PoolService) CreatePoolWithVolumes(ctx context.Context, name string, volumes []string) error {
    return s.db.WithTransaction(ctx, func(tx database.Transaction) error {
        // Create pool
        pool := &datamodel.Pool{Name: name}
        if err := tx.GORM().Create(pool).Error; err != nil {
            return err
        }
        
        // Create volumes
        for _, volName := range volumes {
            if err := tx.GORM().Create(&datamodel.Volume{
                Name:   volName,
                PoolID: pool.ID,
            }).Error; err != nil {
                return err
            }
        }
        return nil
    })
}
```
