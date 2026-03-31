package common

import "os"

type PoolConfig struct {
	PoolName       string `json:"poolName"`
	VolumeName     string `json:"volumeName"`
	DestPoolName   string `json:"destPoolName"`
	ReplicationID1 string `json:"replicationID1"`
	ReplicationID2 string `json:"replicationID2"`
}

func SetPoolConfigEnv(cfg PoolConfig) {
	if cfg.PoolName != "" {
		os.Setenv("POOLNAME", cfg.PoolName)
	}
	if cfg.VolumeName != "" {
		os.Setenv("VOLUMENAME", cfg.VolumeName)
	}
	if cfg.DestPoolName != "" {
		os.Setenv("DESTPOOLNAME", cfg.DestPoolName)
	}
	if cfg.ReplicationID1 != "" {
		os.Setenv("REPLICATIONID1", cfg.ReplicationID1)
	}
	if cfg.ReplicationID2 != "" {
		os.Setenv("REPLICATIONID2", cfg.ReplicationID2)
	}
}

var DocsMap = map[string]string{
	"BILLING ONBOARDING SANITY CHECKS RESULTS": "https://confluence.ngage.netapp.com/spaces/VSCP/pages/1338924354/Billing+Metrics+Sanity+Checks+Setup#Billing%26MetricsSanityChecksSetup-BillingScenariosCoveredSoFar",
	"CRR BILLING SANITY CHECKS RESULTS":        "https://confluence.ngage.netapp.com/spaces/VSCP/pages/1338924354/Billing+Metrics+Sanity+Checks+Setup",
	"BACKUP BILLING SANITY CHECKS RESULTS":     "https://netapp.atlassian.net/wiki/spaces/~ksharans/pages/566832017/Backup+Billing+Metrics+Validation+-+Test+Design+Specification+TDS",
	"POOL BILLING SANITY CHECKS RESULTS":       "https://confluence.ngage.netapp.com/spaces/VSCP/pages/1378911888/Pool+Billing+Metrics+validation+-+Test+Design+Specification+TDS",
	"VOLUME BILLING SANITY CHECKS RESULTS":     "https://confluence.ngage.netapp.com/spaces/VSCP/pages/1378911986/Volume+Billing+Metrics+Validation+-+Test+Design+Specification+TDS",
	"AT BILLING SANITY CHECKS RESULTS":         "https://confluence.ngage.netapp.com/spaces/VSCP/pages/1338924354/Billing+Metrics+Sanity+Checks+Setup",
}
