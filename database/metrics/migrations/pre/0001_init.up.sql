CREATE TABLE IF NOT EXISTS schema_checksums
(
    id         SERIAL PRIMARY KEY,
    checksum   VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_schema_checksums_checksum ON schema_checksums (checksum);
CREATE INDEX IF NOT EXISTS idx_schema_checksums_created_at ON schema_checksums (created_at);