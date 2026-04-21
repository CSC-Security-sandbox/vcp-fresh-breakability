CREATE TABLE IF NOT EXISTS address_ranges (
    id                           BIGSERIAL PRIMARY KEY,
    uuid                         VARCHAR(36) NOT NULL UNIQUE,
    name                         TEXT NOT NULL,
    address_range_cidr           TEXT NOT NULL,
    network                      TEXT NOT NULL,
    vpc_name                     TEXT NOT NULL,
    host_project_number          TEXT NOT NULL,
    lif_type                     TEXT NOT NULL DEFAULT 'dataLIF',
    address_range_state          TEXT NOT NULL DEFAULT 'CREATED',
    address_range_state_details  TEXT,
    apply_route_aggregation      BOOLEAN NOT NULL DEFAULT FALSE,
    route_aggregation_applied    BOOLEAN NOT NULL DEFAULT FALSE,
    route_aggregation_applied_at TIMESTAMPTZ,
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at                   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_address_ranges_host_project ON address_ranges(host_project_number);
CREATE INDEX IF NOT EXISTS idx_address_ranges_vpc ON address_ranges(vpc_name);

-- Partial unique indexes prevent duplicate active records; the WHERE deleted_at IS NULL
-- clause allows the same CIDR/name to be reused after a soft-delete.
CREATE UNIQUE INDEX IF NOT EXISTS idx_address_ranges_unique_cidr
    ON address_ranges(vpc_name, host_project_number, address_range_cidr)
    WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_address_ranges_unique_name
    ON address_ranges(vpc_name, host_project_number, name)
    WHERE deleted_at IS NULL;

-- At most one interclusterLIF per VPC + host project.
CREATE UNIQUE INDEX IF NOT EXISTS idx_address_ranges_unique_iclif
    ON address_ranges(vpc_name, host_project_number, lif_type)
    WHERE deleted_at IS NULL AND lif_type = 'interclusterLIF';
