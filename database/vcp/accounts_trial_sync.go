package database

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
)

// TrialSyncEligibleFilter returns a PostgreSQL GetAccountsWithFilter for trial account sync:
// enabled state, trialMode object with start/end times, and no exitReason.
func TrialSyncEligibleFilter() *dbutils.Filter {
	return dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("state", "=", datamodel.AccountStateEnabled),
		dbutils.NewFilterCondition("jsonb_typeof(account_metadata->'trialMode')", "=", "object"),
		dbutils.NewFilterCondition("NULLIF(btrim(account_metadata->'trialMode'->>'startTime'), '')", "<>", ""),
		dbutils.NewFilterCondition("NULLIF(btrim(account_metadata->'trialMode'->>'endTime'), '')", "<>", ""),
		dbutils.NewFilterCondition("COALESCE(NULLIF(btrim(account_metadata->'trialMode'->>'exitReason'), ''), '')", "=", ""),
	)
}
