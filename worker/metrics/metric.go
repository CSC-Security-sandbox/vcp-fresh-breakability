package metrics

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/helper"
)

type VolumeDetails struct {
	Name        string
	State       string
	AccountID   int64
	AccountName string
}

type BackupDetailForMetric struct {
	VolName     string
	AccountName string
	Size        int64
}

var JobStatusCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "vcp_job_status_updates",
		Help: "Total number of job status updates",
	},
	[]string{"project_id", "error_details", "state"},
)

// Gauge for AutoTier enabled
var autoTierEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_vsa_autotier_volume",
		Help: "Total number of volumes with autotier enabled",
	},
	[]string{"name", "state", "account_name"},
)

// Gauge for large volume enabled
var largeVolumeEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_vsa_large_volume",
		Help: "Total number of volumes with large volume enabled",
	},
	[]string{"name", "state", "account_name"},
)

// Gauge for CBS enabled
var cbsEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_vsa_cbs_volume",
		Help: "Total number of volumes with CBS enabled",
	},
	[]string{"name", "state", "account_name"},
)

// Gauge for CRR enabled
var crrEnabledGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_vsa_crr_volume",
		Help: "Total number of volumes with CRR enabled",
	},
	[]string{"name", "state", "account_name"},
)

// Gauge for eligibility string volumes
var eligibilityStringGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_volumes_eligibility",
		Help: "Total number of volumes for eligibility string",
	},
	[]string{"name", "state"},
)

// Gauge for backup size
var backupSizeGauge = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "gcnv_backup_size_bytes",
		Help: "Total size of the backups in bytes",
	},
	[]string{"name", "account_name"},
)

func IncJobStatusCounter(ctx context.Context, errorDetails, state string) {
	projectID := helper.GetProjectID(ctx)
	if len(errorDetails) > 1024 {
		errorDetails = errorDetails[:1024]
	}
	JobStatusCounter.WithLabelValues(
		projectID,
		errorDetails,
		state,
	).Inc()
}

func RegisterJobStatusCounter() {
	err := prometheus.Register(JobStatusCounter)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			JobStatusCounter = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			log.Printf("Failed to register JobStatusCounter: %v", err)
		}
	}
}

func RegisterAutoTierEnabledGauge() {
	err := prometheus.Register(autoTierEnabledGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			autoTierEnabledGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register autoTierEnabledGauge: %v", err)
		}
	}
}

func RegisterLargeVolumeEnabledGauge() {
	err := prometheus.Register(largeVolumeEnabledGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			largeVolumeEnabledGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register largeVolumeEnabledGauge: %v", err)
		}
	}
}

func RegisterCRREnabledGauge() {
	err := prometheus.Register(crrEnabledGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			crrEnabledGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register crrEnabledGauge: %v", err)
		}
	}
}

func RegisterCBSEnabledGauge() {
	err := prometheus.Register(cbsEnabledGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			cbsEnabledGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register cbsEnabledGauge: %v", err)
		}
	}
}

func RegisterEligibilityStringGauge() {
	err := prometheus.Register(eligibilityStringGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			eligibilityStringGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register eligibilityStringGauge: %v", err)
		}
	}
}

func RegisterBackupSizeGauge() {
	err := prometheus.Register(backupSizeGauge)
	if err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			backupSizeGauge = are.ExistingCollector.(*prometheus.GaugeVec)
		} else {
			log.Printf("Failed to register backupSizeGauge: %v", err)
		}
	}
}

// Aggregate and emit metrics for Autotier enabled volumes

func EmitAutoTierEnabledMetric(volumes []*datamodel.Volume) {
	autoTierEnabledGauge.Reset()
	type autoTierKey struct {
		Name        string
		State       string
		AccountName string
	}
	counts := make(map[autoTierKey]int)
	for _, v := range volumes {
		if v.AutoTieringEnabled {
			accountName := ""
			if v.Account != nil {
				accountName = v.Account.Name
			}
			key := autoTierKey{
				Name:        v.Name,
				State:       v.State,
				AccountName: accountName,
			}
			counts[key]++
		}
	}
	for key, count := range counts {
		autoTierEnabledGauge.WithLabelValues(
			key.Name,
			key.State,
			key.AccountName,
		).Set(float64(count))
	}
}

// Aggregate and emit metrics for CRR enabled volumes

func EmitCRREnabledMetric(volumes []*datamodel.Volume) {
	crrEnabledGauge.Reset()
	type crrKey struct {
		Name        string
		State       string
		AccountName string
	}
	counts := make(map[crrKey]int)
	for _, v := range volumes {
		if v.SnapshotPolicy != nil && v.SnapshotPolicy.IsEnabled {
			accountName := ""
			if v.Account != nil {
				accountName = v.Account.Name
			}
			key := crrKey{
				Name:        v.Name,
				State:       v.State,
				AccountName: accountName,
			}
			counts[key]++
		}
	}
	for key, count := range counts {
		crrEnabledGauge.WithLabelValues(
			key.Name,
			key.State,
			key.AccountName,
		).Set(float64(count))
	}
}

// Aggregate and emit metrics for LargeVolume enabled volumes

func EmitLargeVolumeEnabledMetric(volumes []*datamodel.Volume) {
	largeVolumeEnabledGauge.Reset()
	type largeVolumeKey struct {
		Name        string
		State       string
		AccountName string
	}
	counts := make(map[largeVolumeKey]int)
	for _, v := range volumes {
		if v.LargeVolumeAttributes != nil && v.LargeVolumeAttributes.LargeCapacity {
			accountName := ""
			if v.Account != nil {
				accountName = v.Account.Name
			}
			key := largeVolumeKey{
				Name:        v.Name,
				State:       v.State,
				AccountName: accountName,
			}
			counts[key]++
		}
	}
	for key, count := range counts {
		largeVolumeEnabledGauge.WithLabelValues(
			key.Name,
			key.State,
			key.AccountName,
		).Set(float64(count))
	}
}

// Aggregate and emit metrics for CBS enabled volumes

func EmitCBSEnabledMetric(volumes []*datamodel.Volume) {
	cbsEnabledGauge.Reset()
	type cbsKey struct {
		Name        string
		State       string
		AccountName string
	}
	counts := make(map[cbsKey]int)
	for _, v := range volumes {
		if v.DataProtection != nil && v.DataProtection.BackupVaultID != "" {
			accountName := ""
			if v.Account != nil {
				accountName = v.Account.Name
			}
			key := cbsKey{
				Name:        v.Name,
				State:       v.State,
				AccountName: accountName,
			}
			counts[key]++
		}
	}
	for key, count := range counts {
		cbsEnabledGauge.WithLabelValues(
			key.Name,
			key.State,
			key.AccountName,
		).Set(float64(count))
	}
}

// Aggregate and emit metrics for Eligibility String

func EmitEligibilityStringMetric(volumes []*datamodel.Volume) {
	eligibilityStringGauge.Reset()
	type eligibilityKey struct {
		Name  string
		State string
	}
	counts := make(map[eligibilityKey]int)
	for _, v := range volumes {
		key := eligibilityKey{
			Name:  v.Name,
			State: v.State,
		}
		counts[key]++
	}
	for key, count := range counts {
		eligibilityStringGauge.WithLabelValues(
			key.Name,
			key.State,
		).Set(float64(count))
	}
}

// Aggregate and emit metrics for Backup Size
func EmitBackupDetailsMetric(details []BackupDetailForMetric) {
	backupSizeGauge.Reset()
	for _, d := range details {
		backupSizeGauge.WithLabelValues(
			d.VolName,
			d.AccountName,
		).Set(float64(d.Size))
	}
}
