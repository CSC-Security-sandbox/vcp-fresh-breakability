# Database Migration System

## Overview
This project uses a sophisticated three-phase migration system that combines SQL migrations with GORM's AutoMigrate feature. The system is designed to handle complex database schema changes while maintaining data integrity and supporting both forward and backward migrations.

## Migration Architecture

### Three-Phase Migration Process
1. **Pre-migration SQL files**: Run before GORM AutoMigrate (schema structure changes)
2. **GORM AutoMigrate**: Automatic schema updates based on Go struct changes
3. **Post-migration SQL files**: Run after GORM AutoMigrate (data changes, indexes, constraints)

### Folder Structure
```
database/postgres/migrations/
├── core/
│   ├── pre/     - Pre-migration files (run before GORM AutoMigrate)
│   │   ├── 0001_init.up.sql
│   │   ├── 0001_init.down.sql
│   │   ├── 0002_add_tables.up.sql
│   │   └── 0002_add_tables.down.sql
│   └── post/    - Post-migration files (run after GORM AutoMigrate)
│       ├── 0001_fill_deployment_name.up.sql
│       ├── 0001_fill_deployment_name.down.sql
│       ├── 0002_update_cluster_name.up.sql
│       └── 0002_update_cluster_name.down.sql
└── README.md
```

## Migration Types

### Pre-Migrations (`migrations/core/pre/`)
**Purpose**: Structural database changes that need to happen before GORM processes Go structs.

**Use cases**:
- Creating tables that GORM doesn't manage
- Adding custom database types or extensions
- Creating stored procedures or functions
- Complex schema modifications that GORM can't handle
- Database-specific optimizations

**Example**:
```sql
-- 0001_init.up.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TYPE deployment_status AS ENUM ('pending', 'running', 'failed', 'completed');
```

### GORM AutoMigrate
**Purpose**: Automatic schema updates based on Go struct definitions.

**Handles**:
- Creating tables from Go structs
- Adding/removing columns based on struct fields
- Index creation from struct tags
- Basic foreign key relationships

**Tracked in**: `schema_checksums` table (tracks Go struct changes)

### Post-Migrations (`migrations/core/post/`)
**Purpose**: Data changes and optimizations that require the final schema state.

**Use cases**:
- Data migrations and transformations
- Creating complex indexes after data is populated
- Adding constraints that depend on data state
- Creating views that depend on final schema
- Performance optimizations
- Data cleanup and validation

**Example**:
```sql
-- 0001_fill_deployment_name.up.sql
UPDATE pools SET deployment_name = 'default' WHERE deployment_name IS NULL;
ALTER TABLE pools ALTER COLUMN deployment_name SET NOT NULL;
```

## Migration Tracking

### Separate Tracking Tables
- **Pre-migrations**: Tracked in `schema_migrations` table
- **Post-migrations**: Tracked in `schema_migrations_post` table
- **GORM changes**: Tracked in `schema_checksums` table

This separation prevents version conflicts and allows independent rollback of each migration type.

### Version Management
- **Pre-migrations**: Use standard golang-migrate versioning (e.g., 0001, 0002, 0003)
- **Post-migrations**: Use independent sequential versioning (e.g., 0001, 0002, 0003)
- **No version conflicts**: Each migration type has its own version namespace

## Creating New Migrations

### Pre-Migration Files
```bash
# Create a new pre-migration
touch migrations/core/pre/0003_add_custom_types.up.sql
touch migrations/core/pre/0003_add_custom_types.down.sql
```

### Post-Migration Files
```bash
# Create a new post-migration
touch migrations/core/post/0003_optimize_indexes.up.sql
touch migrations/core/post/0003_optimize_indexes.down.sql
```

### Naming Convention
- Format: `NNNN_description.up.sql` and `NNNN_description.down.sql`
- Use sequential version numbers within each folder
- Use descriptive names that explain the migration purpose
- Keep descriptions short but clear

## Migration Execution Order

1. **Pre-migration SQL files**: Files in `migrations/core/pre/` (via golang-migrate)
2. **GORM AutoMigrate**: Automatic schema updates based on Go struct changes
3. **Post-migration SQL files**: Files in `migrations/core/post/` (via golang-migrate)
4. **Post-migration fixes**: Any additional programmatic fixes

## Error Handling and Rollback

### Dirty State Recovery
If a migration fails, it may leave the database in a "dirty" state:

```sql
-- Check migration status
SELECT version, dirty FROM schema_migrations ORDER BY version;
SELECT version, dirty FROM schema_migrations_post ORDER BY version;

-- Clean dirty state (choose appropriate option)
UPDATE schema_migrations_post SET dirty = false WHERE version = X;
-- OR
DELETE FROM schema_migrations_post WHERE version = X;
```

### Rollback Strategy
- **Pre-migrations**: Can be rolled back using standard golang-migrate
- **GORM AutoMigrate**: Cannot be easily rolled back (design structs carefully)
- **Post-migrations**: Can be rolled back using `.down.sql` files

## Best Practices

### General Guidelines
1. **Test migrations thoroughly** on a copy of production data
2. **Make migrations idempotent** when possible
3. **Keep migrations small and focused** on one specific change
4. **Always create both `.up.sql` and `.down.sql` files**
5. **Use transactions** for multi-step migrations when appropriate

### Pre-Migration Best Practices
- Use `IF NOT EXISTS` clauses to make migrations idempotent
- Create database extensions and custom types here
- Handle complex schema changes that GORM can't manage

### Post-Migration Best Practices
- Use `WHERE` clauses to make data updates idempotent
- Create indexes `CONCURRENTLY` to avoid blocking
- Validate data integrity after major changes
- Use `COALESCE` and `NULLIF` for safe data transformations

### Performance Considerations
- Create indexes in post-migrations after data is populated
- Use `ANALYZE` after major data changes
- Consider using `VACUUM` for large deletions
- Monitor long-running migrations in production

## Troubleshooting

### Common Issues
1. **Dirty database state**: Use the cleanup SQL provided above
2. **Migration conflicts**: Ensure unique version numbers within each folder
3. **Data migration failures**: Check for NULL values and data consistency
4. **Performance issues**: Monitor long-running migrations, consider batching

### Debugging Tips
- Check migration status in both tracking tables
- Test migrations on a copy of production data
- Use `EXPLAIN` to analyze query performance
- Monitor database locks during migrations

## Migration Testing

### Local Testing
```bash
# Run migrations
go run ./cmd/migrate

# Check migration status
# Connect to database and run:
SELECT version, dirty FROM schema_migrations;
SELECT version, dirty FROM schema_migrations_post;
```

### Production Deployment
1. **Backup database** before running migrations
2. **Test on staging** environment first
3. **Monitor migration progress** and performance
4. **Have rollback plan** ready
5. **Validate data integrity** after completion

This system provides a robust, maintainable approach to database schema evolution while supporting complex data transformations and performance optimizations.
