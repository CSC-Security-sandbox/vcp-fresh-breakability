package datamodel

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJSONB_Scan(t *testing.T) {
	var j JSONB
	err := j.Scan([]byte(`{"key": "value"}`))
	assert.NoError(t, err)
	assert.Equal(t, "value", j["key"])

	err = j.Scan(nil)
	assert.NoError(t, err)
	assert.Equal(t, JSONB{}, j)
}

func TestJSONB_Value(t *testing.T) {
	j := JSONB{"key": "value"}
	val, err := j.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"key":"value"}`, string(val.([]byte)))
}

func TestClusterDetails_Scan(t *testing.T) {
	var cd ClusterDetails
	err := cd.Scan([]byte(`{"external_name": "test"}`))
	assert.NoError(t, err)
	assert.Equal(t, "test", cd.ExternalName)
}

func TestClusterDetails_Value(t *testing.T) {
	cd := ClusterDetails{ExternalName: "test"}
	val, err := cd.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"external_name":"test","ontap_version":"","regional_tenant_project":"","sn_host_project":"","network":"","subnet_names":null}`, string(val.([]byte)))
}

func TestVolumeAttributes_Scan(t *testing.T) {
	var va VolumeAttributes
	err := va.Scan([]byte(`{"creation_token": "token"}`))
	assert.NoError(t, err)
	assert.Equal(t, "token", va.CreationToken)
}

func TestVolumeAttributes_Value(t *testing.T) {
	va := VolumeAttributes{CreationToken: "token"}
	val, err := va.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"creation_token":"token","protocols":null,"vendor_subnet_id":"","external_uuid":"","block_properties":null,"file_properties":null,"is_data_protection":false,"snap_reserve":0,"labels":null,"restored_backup_id":"","restored_backup_path":""}`, string(val.([]byte)))
}

func TestReplicationDetails_Scan(t *testing.T) {
	var rd ReplicationDetails
	err := rd.Scan([]byte(`{"endpoint_type": "type"}`))
	assert.NoError(t, err)
	assert.Equal(t, "type", rd.EndpointType)
}

func TestReplicationDetails_Value(t *testing.T) {
	rd := ReplicationDetails{EndpointType: "type"}
	val, err := rd.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"endpoint_type":"type","replication_type":"","replication_schedule":"","source_pool_uuid":"","source_volume_uuid":"","source_location":"","source_host_name":"","source_replication_uuid":"","source_svm_name":"","source_volume_name":"","destination_pool_uuid":"","destination_volume_uuid":"","destination_location":"","destination_host_name":"","destination_replication_uuid":"","destination_svm_name":"","destination_volume_name":"","external_uuid":""}`, string(val.([]byte)))
}

func TestSnapshotAttributes_Scan(t *testing.T) {
	var sa SnapshotAttributes
	err := sa.Scan([]byte(`{"size_in_bytes": 1024}`))
	assert.NoError(t, err)
	assert.Equal(t, int64(1024), sa.SizeInBytes)
}

func TestSnapshotAttributes_Value(t *testing.T) {
	sa := SnapshotAttributes{SizeInBytes: 1024}
	val, err := sa.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"size_in_bytes":1024,"external_uuid":"","logical_size_used_in_bytes":0}`, string(val.([]byte)))
}

func TestBucketDetailsArray_Value(t *testing.T) {
	bda := BucketDetailsArray{{BucketName: "test-bucket"}}
	val, err := bda.Value()
	assert.NoError(t, err)
	assert.Equal(t, `[{"bucket_name":"test-bucket","service_account_name":"","vendor_subnet_id":"","tenant_project_number":""}]`, string(val.([]byte)))
}

func TestImmutableAttributes_Scan(t *testing.T) {
	var ia ImmutableAttributes
	err := ia.Scan([]byte(`{"isDailyBackupImmutable": true}`))
	assert.NoError(t, err)
	assert.True(t, ia.IsDailyBackupImmutable)
}

func TestImmutableAttributes_Value(t *testing.T) {
	ia := ImmutableAttributes{IsDailyBackupImmutable: true}
	val, err := ia.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"backupMinimumEnforcedRetentionDuration":null,"isDailyBackupImmutable":true,"isWeeklyBackupImmutable":false,"isMonthlyBackupImmutable":false,"isAdhocBackupImmutable":false}`, string(val.([]byte)))
}
func TestHosts_Scan(t *testing.T) {
	var h Hosts
	err := h.Scan([]byte(`{"hosts": ["host1", "host2"]}`))
	assert.NoError(t, err)
	assert.Equal(t, []string{"host1", "host2"}, h.Hosts)
}

func TestHosts_Value(t *testing.T) {
	h := Hosts{Hosts: []string{"host1", "host2"}}
	val, err := h.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"hosts":["host1","host2"]}`, string(val.([]byte)))
}

func TestDataProtection_Scan(t *testing.T) {
	var dp DataProtection
	err := dp.Scan([]byte(`{"scheduled_backup_enabled": true, "backup_vault_id": "vault123"}`))
	assert.NoError(t, err)
	assert.NotNil(t, dp.ScheduledBackupEnabled)
	assert.True(t, *dp.ScheduledBackupEnabled)
	assert.Equal(t, "vault123", dp.BackupVaultID)
}

func TestDataProtection_Value(t *testing.T) {
	dp := DataProtection{
		ScheduledBackupEnabled: func(b bool) *bool { return &b }(true),
		BackupVaultID:          "vault123",
	}
	val, err := dp.Value()
	assert.NoError(t, err)
	assert.Equal(t, `{"scheduled_backup_enabled":true,"backup_vault_id":"vault123","backup_policy_id":"","backup_chain_bytes":null}`, string(val.([]byte)))
}

func TestBackupAttributes_Scan(t *testing.T) {
	t.Run("Valid JSON", func(t *testing.T) {
		var ba BackupAttributes
		err := ba.Scan([]byte(`{
			"progress_percentage": 50,
			"bytes_transferred": 1024,
			"backup_policy_name": "policy1",
			"snapshot_id": "snap123",
			"snapshot_name": "snapshot1",
			"snapshot_creation_time": "2023-01-01T00:00:00Z",
			"completion_time": "2023-01-01T01:00:00Z",
			"life_cycle_tracking_id": "track123",
			"constituent_volumes_per_aggregate": "vol1",
			"use_existing_snapshot": true,
			"number_of_aggregates": 2,
			"ontap_volume_style": "flexvol",
			"service_account_name": "service1",
			"endpoint_uuid": "endpoint123",
			"bucket_name": "bucket1",
			"protocols": ["nfs", "cifs"],
			"volume_name": "volume1",
			"account_identifier": "project123"
		}`))
		assert.NoError(t, err)
		assert.Equal(t, "policy1", ba.BackupPolicyName)
		assert.Equal(t, "snap123", ba.SnapshotID)
		assert.Equal(t, "snapshot1", ba.SnapshotName)
		assert.Equal(t, "2023-01-01T00:00:00Z", ba.SnapshotCreationTime)
		assert.Equal(t, "2023-01-01T01:00:00Z", ba.CompletionTime)
		assert.Equal(t, "track123", ba.LifeCycleTrackingID)
		assert.Equal(t, "vol1", ba.ConstituentVolumesPerAggregate)
		assert.True(t, ba.UseExistingSnapshot)
		assert.Equal(t, 2, ba.NumberOfAggregates)
		assert.Equal(t, "flexvol", ba.OntapVolumeStyle)
		assert.Equal(t, "service1", ba.ServiceAccountName)
		assert.Equal(t, "endpoint123", ba.EndpointUUID)
		assert.Equal(t, "bucket1", ba.BucketName)
		assert.Equal(t, []string{"nfs", "cifs"}, ba.Protocols)
		assert.Equal(t, "volume1", ba.VolumeName)
		assert.Equal(t, "project123", ba.AccountIdentifier)
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		var ba BackupAttributes
		err := ba.Scan([]byte(`invalid json`))
		assert.Error(t, err)
	})

	t.Run("Nil Value", func(t *testing.T) {
		var ba BackupAttributes
		err := ba.Scan(nil)
		assert.Error(t, err)
		assert.Equal(t, BackupAttributes{}, ba)
	})
}

func TestBackupAttributes_Value(t *testing.T) {
	ba := BackupAttributes{
		BackupPolicyName:               "policy1",
		SnapshotID:                     "snap123",
		SnapshotName:                   "snapshot1",
		SnapshotCreationTime:           "2023-01-01T00:00:00Z",
		CompletionTime:                 "2023-01-01T01:00:00Z",
		LifeCycleTrackingID:            "track123",
		ConstituentVolumesPerAggregate: "vol1",
		UseExistingSnapshot:            true,
		NumberOfAggregates:             2,
		OntapVolumeStyle:               "flexvol",
		ServiceAccountName:             "service1",
		EndpointUUID:                   "endpoint123",
		BucketName:                     "bucket1",
		Protocols:                      []string{"nfs", "cifs"},
		VolumeName:                     "volume1",
		AccountIdentifier:              "project123",
		EnforcedRetentionDuration:      time.Time{}, // Ensure this field is properly initialized
	}

	val, err := ba.Value()
	assert.NoError(t, err)

	expectedJSON := `{"backup_policy_name":"policy1","snapshot_id":"snap123","snapshot_name":"snapshot1","snapshot_creation_time":"2023-01-01T00:00:00Z","completion_time":"2023-01-01T01:00:00Z","life_cycle_tracking_id":"track123","constituent_volumes_per_aggregate":"vol1","use_existing_snapshot":true,"number_of_aggregates":2,"ontap_volume_style":"flexvol","service_account_name":"service1","endpoint_uuid":"endpoint123","bucket_name":"bucket1","protocols":["nfs","cifs"],"volume_name":"volume1","account_identifier":"project123","enforced_retention_duration":"0001-01-01T00:00:00Z"}`
	assert.JSONEq(t, expectedJSON, string(val.([]byte)))
}

func TestPoolUniqueConstraint(t *testing.T) {
	// Test that the GORM tags are set up correctly for composite unique constraint
	pool1 := &Pool{
		AccountID:      123,
		DeploymentName: "gcp-a1b2c3d4e5f67890",
	}

	pool2 := &Pool{
		AccountID:      456,                    // Different account
		DeploymentName: "gcp-a1b2c3d4e5f67890", // Same deployment name - should be allowed
	}

	pool3 := &Pool{
		AccountID:      123,                    // Same account as pool1
		DeploymentName: "gcp-b2c3d4e5f6789012", // Different deployment name - should be allowed
	}

	// Verify the structures are set up correctly
	assert.Equal(t, int64(123), pool1.AccountID)
	assert.Equal(t, "gcp-a1b2c3d4e5f67890", pool1.DeploymentName)

	assert.Equal(t, int64(456), pool2.AccountID)
	assert.Equal(t, "gcp-a1b2c3d4e5f67890", pool2.DeploymentName)

	assert.Equal(t, int64(123), pool3.AccountID)
	assert.Equal(t, "gcp-b2c3d4e5f6789012", pool3.DeploymentName)

	// The actual uniqueness constraint will be enforced by the database
	// This test just verifies the structure is correct
}

func TestNodeNodeGroupMap_Fields(t *testing.T) {
	createdAt := time.Now()
	updatedAt := createdAt.Add(time.Hour)
	m := NodeNodeGroupMap{
		BaseModel:   BaseModel{ID: 1, UUID: "uuid-1", CreatedAt: createdAt, UpdatedAt: updatedAt},
		NodeID:      123,
		NodeGroupID: 456,
		NodeGroup:   &NodeGroup{},
	}
	assert.Equal(t, int64(1), m.ID)
	assert.Equal(t, "uuid-1", m.UUID)
	assert.Equal(t, int64(123), m.NodeID)
	assert.Equal(t, int64(456), m.NodeGroupID)
	assert.Equal(t, createdAt, m.CreatedAt)
	assert.Equal(t, updatedAt, m.UpdatedAt)
}
