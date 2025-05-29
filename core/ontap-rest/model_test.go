package ontap_rest

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGcpKmsCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := gcpKmsCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &GcpKmsCreateParams{
			KeyName:                nillable.ToPointer("keyname"),
			KeyRingLocation:        nillable.ToPointer("ringlocation"),
			KeyRingName:            nillable.ToPointer("ringname"),
			ProjectID:              nillable.ToPointer("projectid"),
			ApplicationCredentials: nillable.ToPointer(strfmt.Password("credentials")),
			SvmName:                nillable.ToPointer("svmname"),
		}

		otParams := gcpKmsCreateParamsToONTAP(params)
		assert.Equal(tt, "keyname", *otParams.Info.KeyName)
		assert.Equal(tt, "ringlocation", *otParams.Info.KeyRingLocation)
		assert.Equal(tt, "ringname", *otParams.Info.KeyRingName)
		assert.Equal(tt, "projectid", *otParams.Info.ProjectID)
		assert.Equal(tt, "credentials", string(*otParams.Info.ApplicationCredentials))
		assert.Equal(tt, "svmname", *otParams.Info.Svm.Name)
	})
}

func TestAggregateCollectionGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := aggregateCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &AggregateCollectionGetParams{
			Name: nillable.ToPointer("name"),
		}
		otParams := aggregateCollectionGetParamsToONTAP(params)
		assert.Equal(tt, "name", *otParams.Name)
	})
}

func TestAggregateModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := aggregateModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &AggregateModifyParams{
			UUID:                     "uuid",
			TieringFullnessThreshold: nillable.ToPointer(int64(616)),
		}
		otParams := aggregateModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.Equal(tt, int64(616), *otParams.Info.CloudStorage.TieringFullnessThreshold)
	})
}

func TestVolumeModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := volumeModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenQuotaEnabledSet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:         "uuid",
			QuotaEnabled: nillable.ToPointer(false),
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.False(tt, *otParams.Info.Quota.Enabled)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Clone)
	})
	t.Run("WhenReKeySet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:  "uuid",
			ReKey: nillable.ToPointer(false),
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.False(tt, *otParams.Info.Encryption.Rekey)
		assert.Nil(tt, otParams.Info.Quota)
		assert.Nil(tt, otParams.Info.Clone)
	})
	t.Run("WhenSplitInitiatedSet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:           "uuid",
			SplitInitiated: nillable.ToPointer(false),
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.False(tt, *otParams.Info.Clone.SplitInitiated)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Quota)
		assert.Nil(tt, otParams.ReturnTimeout)
	})
	t.Run("WhenSnapshotPolicyNameSet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:               "uuid",
			SnapshotPolicyName: nillable.GetStringPtr("my-snapshot"),
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.Equal(tt, params.SnapshotPolicyName, otParams.Info.SnapshotPolicy.Name)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Quota)
	})
	t.Run("WhenRestoreToSnapshotUUIDSet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:                  "uuid",
			RestoreToSnapshotUUID: nillable.ToPointer("snapshotUUID"),
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.Equal(tt, "snapshotUUID", *otParams.RestoreToSnapshotUUID)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Quota)
		assert.Nil(tt, otParams.Info.Clone)
	})
	t.Run("WhenMovementIsSet", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID: "uuid",
			Movement: &VolumeMovementParams{
				VolumeMovementDestinationAggregate: &VolumeMovementDestinationAggregate{
					DestinationAggregateUUID: nillable.ToPointer("someAggregateUUID"),
					DestinationAggregateName: nillable.ToPointer("someAggregateName"),
				},
				TieringPolicy: nillable.ToPointer("none"),
			},
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.NotNil(tt, otParams.Info.Movement)
		assert.Equal(tt, "someAggregateUUID", *otParams.Info.Movement.DestinationAggregate.UUID)
		assert.Equal(tt, "someAggregateName", *otParams.Info.Movement.DestinationAggregate.Name)
		assert.Equal(tt, "none", *otParams.Info.Movement.TieringPolicy)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Quota)
		assert.Nil(tt, otParams.Info.Clone)
	})
	t.Run("WhenMovementStateIsSet", func(tt *testing.T) {
		state := string(models.VolumeInlineMovementStateAborted)
		params := &VolumeModifyParams{
			UUID: "uuid",
			Movement: &VolumeMovementParams{
				State: &state,
			},
		}
		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.NotNil(tt, otParams.Info.Movement)
		assert.Nil(tt, otParams.Info.Movement.DestinationAggregate)
		assert.Nil(tt, otParams.Info.Movement.TieringPolicy)
		assert.Equal(tt, "aborted", *otParams.Info.Movement.State)
		assert.Nil(tt, otParams.Info.Encryption)
		assert.Nil(tt, otParams.Info.Quota)
		assert.Nil(tt, otParams.Info.Clone)
	})
	t.Run("WhenVolumeUpdaterParams", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:                           "uuid",
			Comment:                        nillable.ToPointer("a comment"),
			SecurityStyle:                  nillable.ToPointer("security style"),
			UnixPermissions:                nillable.ToPointer("0777"),
			Size:                           nillable.ToPointer(uint64(1111)),
			LogicalSpaceEnforcement:        nillable.ToPointer(false),
			SnapReserve:                    nillable.ToPointer(2222),
			MaxFiles:                       nillable.ToPointer(uint64(3333)),
			SnapshotDirectoryAccessEnabled: nillable.ToPointer(true),
			SetAtTimeEnabled:               nillable.ToPointer(false),
			TieringPolicy:                  nillable.ToPointer("tiering policy"),
			TieringMinimumCoolingDays:      nillable.ToPointer(int32(4444)),
			CloudRetrievalPolicy:           nillable.ToPointer("cloud retrieval policy"),
			SplitInitiated:                 nillable.ToPointer(true),
			MatchParentStorageTier:         true,
			RestoreToSnapshotUUID:          nillable.ToPointer("321"),
			State:                          nillable.ToPointer("stateful"),
			Path:                           nillable.ToPointer("/"),
			SnapshotPolicyName:             nillable.ToPointer("ssp1"),
			ExportPolicy:                   nillable.ToPointer("ep1"),
			QosPolicy:                      nillable.ToPointer("qos"),
		}

		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.Equal(tt, *params.Comment, *otParams.Info.Comment)
		assert.Equal(tt, int64(*params.Size), *otParams.Info.Space.Size)
		assert.False(tt, *otParams.Info.Space.LogicalSpace.Enforcement)
		assert.Equal(tt, int64(*params.SnapReserve), *otParams.Info.Space.Snapshot.ReservePercent)
		assert.Equal(tt, int64(*params.MaxFiles), *otParams.Info.Files.Maximum)
		assert.Equal(tt, *params.SnapshotDirectoryAccessEnabled, *otParams.Info.SnapshotDirectoryAccessEnabled)
		assert.Equal(tt, *params.TieringPolicy, *otParams.Info.Tiering.Policy)
		assert.Equal(tt, int64(*params.TieringMinimumCoolingDays), *otParams.Info.Tiering.MinCoolingDays)
		assert.Equal(tt, *params.CloudRetrievalPolicy, *otParams.Info.CloudRetrievalPolicy)
		assert.Equal(tt, *params.SplitInitiated, *otParams.Info.Clone.SplitInitiated)
		assert.Equal(tt, fmt.Sprintf("%t", params.MatchParentStorageTier), *otParams.CloneMatchParentStorageTier)
		assert.Equal(tt, *params.RestoreToSnapshotUUID, *otParams.RestoreToSnapshotUUID)
		assert.Equal(tt, *params.State, *otParams.Info.State)
		assert.Equal(tt, *params.SnapshotPolicyName, *otParams.Info.SnapshotPolicy.Name)
		assert.Equal(tt, *params.QosPolicy, *otParams.Info.Qos.Policy.Name)
		assert.Nil(tt, otParams.ReturnTimeout)
	})
	t.Run("WhenVolumeUpdaterParamsWithAutoSize", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:                           "uuid",
			Comment:                        nillable.ToPointer("a comment"),
			SecurityStyle:                  nillable.ToPointer("security style"),
			UnixPermissions:                nillable.ToPointer("0777"),
			Size:                           nillable.ToPointer(uint64(1111)),
			MaxAutoSize:                    nillable.ToPointer(uint64(1111)),
			LogicalSpaceEnforcement:        nillable.ToPointer(false),
			SnapReserve:                    nillable.ToPointer(2222),
			MaxFiles:                       nillable.ToPointer(uint64(3333)),
			SnapshotDirectoryAccessEnabled: nillable.ToPointer(true),
			SetAtTimeEnabled:               nillable.ToPointer(false),
			TieringPolicy:                  nillable.ToPointer("tiering policy"),
			TieringMinimumCoolingDays:      nillable.ToPointer(int32(4444)),
			CloudRetrievalPolicy:           nillable.ToPointer("cloud retrieval policy"),
			SplitInitiated:                 nillable.ToPointer(true),
			MatchParentStorageTier:         true,
			RestoreToSnapshotUUID:          nillable.ToPointer("321"),
			State:                          nillable.ToPointer("stateful"),
			Path:                           nillable.ToPointer("/"),
			SnapshotPolicyName:             nillable.ToPointer("ssp1"),
			ExportPolicy:                   nillable.ToPointer("ep1"),
			QosPolicy:                      nillable.ToPointer("qos"),
		}

		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", otParams.UUID)
		assert.Equal(tt, *params.Comment, *otParams.Info.Comment)
		assert.Equal(tt, int64(*params.Size), *otParams.Info.Space.Size)
		assert.Equal(tt, int64(*params.MaxAutoSize), *otParams.Info.Autosize.Maximum)
		assert.False(tt, *otParams.Info.Space.LogicalSpace.Enforcement)
		assert.Equal(tt, int64(*params.SnapReserve), *otParams.Info.Space.Snapshot.ReservePercent)
		assert.Equal(tt, int64(*params.MaxFiles), *otParams.Info.Files.Maximum)
		assert.Equal(tt, *params.SnapshotDirectoryAccessEnabled, *otParams.Info.SnapshotDirectoryAccessEnabled)
		assert.Equal(tt, *params.TieringPolicy, *otParams.Info.Tiering.Policy)
		assert.Equal(tt, int64(*params.TieringMinimumCoolingDays), *otParams.Info.Tiering.MinCoolingDays)
		assert.Equal(tt, *params.CloudRetrievalPolicy, *otParams.Info.CloudRetrievalPolicy)
		assert.Equal(tt, *params.SplitInitiated, *otParams.Info.Clone.SplitInitiated)
		assert.Equal(tt, fmt.Sprintf("%t", params.MatchParentStorageTier), *otParams.CloneMatchParentStorageTier)
		assert.Equal(tt, *params.RestoreToSnapshotUUID, *otParams.RestoreToSnapshotUUID)
		assert.Equal(tt, *params.State, *otParams.Info.State)
		assert.Equal(tt, *params.SnapshotPolicyName, *otParams.Info.SnapshotPolicy.Name)
		assert.Equal(tt, *params.QosPolicy, *otParams.Info.Qos.Policy.Name)
	})
	t.Run("WhenVolumeUpdaterParamsWhenTieringPolicyNone", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:                      "uuid",
			SetAtTimeEnabled:          nillable.ToPointer(false),
			TieringPolicy:             nillable.ToPointer("none"),
			TieringMinimumCoolingDays: nillable.ToPointer(int32(4444)),
			CloudRetrievalPolicy:      nillable.ToPointer("cloud retrieval policy"),
		}

		otParams := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, *params.TieringPolicy, *otParams.Info.Tiering.Policy)
		assert.Nil(tt, otParams.Info.Tiering.MinCoolingDays)
		assert.Equal(tt, "15", *otParams.ReturnTimeout)
	})
	t.Run("WhenVolumeUpdaterParamsWhenTieringPolicyIsNotPassedWithCoolnessThresholdDays", func(tt *testing.T) {
		params := &VolumeModifyParams{
			UUID:                      "uuid",
			SetAtTimeEnabled:          nillable.ToPointer(false),
			TieringPolicy:             nil,
			TieringMinimumCoolingDays: nillable.ToPointer(int32(4444)),
			CloudRetrievalPolicy:      nillable.ToPointer("cloud retrieval policy"),
		}

		otParams := volumeModifyParamsToONTAP(params)
		assert.Nil(tt, otParams.Info.Tiering.Policy)
		assert.Equal(tt, int64(*params.TieringMinimumCoolingDays), *otParams.Info.Tiering.MinCoolingDays)
	})
}

func TestSnapshotCollectionGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapshotCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapshotCollectionGetParams{
			BaseParams:      BaseParams{Fields: []string{"field"}},
			SnapmirrorLabel: nillable.ToPointer("lab"),
			VolumeUUID:      "uuid",
		}

		otParams := snapshotCollectionGetParamsToONTAP(params)
		assert.Equal(tt, "field", otParams.Fields[0])
		assert.Equal(tt, "uuid", otParams.VolumeUUID)
		assert.Equal(tt, "lab", *otParams.SnapmirrorLabel)
	})
}

func TestSnapshotPolicyGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapshotPolicyGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapshotPolicyGetParams{
			BaseParams: BaseParams{Fields: []string{"field"}},
			UUID:       "uuid",
		}

		otParams := snapshotPolicyGetParamsToONTAP(params)
		assert.Equal(tt, "field", otParams.Fields[0])
		assert.Equal(tt, "uuid", otParams.UUID)
	})
}

func TestVolumeCollectionGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := volumeCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &VolumeCollectionGetParams{
			BaseParams: BaseParams{Fields: []string{"field"}},
			UUID:       nillable.ToPointer("uuid"),
			SvmName:    nillable.ToPointer("svm-1"),
		}

		otParams := volumeCollectionGetParamsToONTAP(params)
		assert.Equal(tt, "field", otParams.Fields[0])
		assert.Equal(tt, "uuid", *otParams.UUID)
		assert.Equal(tt, "svm-1", *otParams.SvmName)
	})
}

func TestVolumeGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := volumeGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &VolumeGetParams{
			BaseParams: BaseParams{Fields: []string{"field"}},
			UUID:       "uuid",
		}

		otParams := volumeGetParamsToONTAP(params)
		assert.Equal(tt, "field", otParams.Fields[0])
		assert.Equal(tt, "uuid", otParams.UUID)
	})
}

func TestSvmModifyParamsToOntap(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := svmModifyParamsToOntap(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenNsSwitchSourceNotSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:  "svm-uuid",
			NsSwitch: nil,
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Nil(tt, otParams.Info.Nsswitch)
	})
	t.Run("WhenNsSwitchSourceSet", func(tt *testing.T) {
		var nsSwitchSources []*models.NsswitchSource
		nsSwitchDBValue := models.NsswitchSource("ldap")
		nsSwitchSources = append(nsSwitchSources, &nsSwitchDBValue)
		params := &SvmModifyParams{
			SvmUUID: "svm-uuid",
			NsSwitch: &NsSwitchSource{
				NsSwitchSourceNamemap:  nsSwitchSources,
				NsSwitchSourcePasswd:   nsSwitchSources,
				NsSwitchSourceNetgroup: nsSwitchSources,
				NsSwitchSourceGroup:    nsSwitchSources,
			},
		}
		res := &models.SvmInlineNsswitch{Group: nsSwitchSources, Hosts: []*models.NsswitchSource(nil), Namemap: nsSwitchSources, Netgroup: nsSwitchSources, Passwd: nsSwitchSources}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Equal(tt, res, otParams.Info.Nsswitch)
	})
	t.Run("WhenNfsOrCifsAllowedNotSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:     "svm-uuid",
			CifsAllowed: nil,
			NfsAllowed:  nil,
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Nil(tt, otParams.Info.Cifs)
		assert.Nil(tt, otParams.Info.Nfs)
	})
	t.Run("WhenNfsOrCifsAllowedSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:     "svm-uuid",
			CifsAllowed: nillable.ToPointer(true),
			NfsAllowed:  nillable.ToPointer(false),
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.True(tt, *otParams.Info.Cifs.Allowed)
		assert.False(tt, *otParams.Info.Nfs.Allowed)
	})
	t.Run("WhenRetentionperiodSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:              "svm-uuid",
			RetentionPeriodHours: nillable.ToPointer(int64(666)),
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Equal(tt, int64(666), *otParams.Info.RetentionPeriod)
	})
}

func TestVolumeDeleteParamsToONTAP(t *testing.T) {
	t.Run("WhenEmptyParams", func(tt *testing.T) {
		otParams := volumeDeleteParamsToONTAP(&VolumeDeleteParams{})
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &VolumeDeleteParams{
			UUID: "vol-1",
		}
		otParams := volumeDeleteParamsToONTAP(params)
		assert.Equal(tt, params.UUID, otParams.UUID)
		assert.Equal(tt, returnTimeout, *otParams.ReturnTimeout)
	})
}

func TestIpspaceDeleteParamsToONTAP(t *testing.T) {
	t.Run("WhenEmptyParams", func(tt *testing.T) {
		otParams := ipspaceDeleteParamsToONTAP(&IpspaceDeleteParams{})
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &IpspaceDeleteParams{
			Name: "policy-1",
		}
		otParams := ipspaceDeleteParamsToONTAP(params)
		assert.Equal(tt, &params.Name, otParams.Name)
	})
}

func TestQosPolicyGroupCollectionGetParamsToONTAPCollectionGet(t *testing.T) {
	t.Run("WhenEmptyParams", func(tt *testing.T) {
		otParams := qosPolicyGroupCollectionGetParamsToONTAPCollectionGet(&QosPolicyGroupCollectionGetParams{})
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &QosPolicyGroupCollectionGetParams{
			BaseParams: BaseParams{Fields: []string{"field1"}},
			Name:       "policy-1",
		}
		otParams := qosPolicyGroupCollectionGetParamsToONTAPCollectionGet(params)
		assert.Equal(tt, []string{"field1"}, otParams.Fields)
		assert.Equal(tt, params.Name, *otParams.Name)
	})
}

func TestClusterPeerToONTAPCreate(t *testing.T) {
	location := time.FixedZone("UTC+3", 3*60*60)
	expTime := strfmt.DateTime(time.Now().In(location))

	date := (*time.Time)(&expTime)

	expTimeStr := date.Format(time.RFC3339)
	expTimePtr := nillable.ToPointer(expTimeStr)

	t.Run("WithExpiryTime", func(tt *testing.T) {
		params := ClusterPeerCreateParams{
			Name:               "test",
			IPAddresses:        []string{"1.2.3.4"},
			ExpiryTime:         expTimePtr,
			GeneratePassphrase: false,
		}
		otParams := clusterPeerToONTAPCreate(params)
		assert.Equal(tt, params.Name, *otParams.Info.Name)
		assert.Equal(tt, *expTimePtr, *otParams.Info.Authentication.ExpiryTime)

		var ipAddresses []string
		for _, ip := range otParams.Info.Remote.IPAddresses {
			if ip != nil {
				ipAddresses = append(ipAddresses, string(*ip))
			}
		}
		assert.Equal(tt, params.IPAddresses, ipAddresses)
	})
	t.Run("WithoutExpiryTime", func(tt *testing.T) {
		params := ClusterPeerCreateParams{
			Name:               "test",
			IPAddresses:        []string{"1.2.3.4"},
			IPSpace:            "ipSpace",
			GeneratePassphrase: false,
		}

		otParams := clusterPeerToONTAPCreate(params)
		assert.Equal(tt, params.Name, *otParams.Info.Name)
		assert.Nil(tt, otParams.Info.Authentication.ExpiryTime)

		var ipAddresses []string
		for _, ip := range otParams.Info.Remote.IPAddresses {
			if ip != nil {
				ipAddresses = append(ipAddresses, string(*ip))
			}
		}
		assert.Equal(tt, params.IPAddresses, ipAddresses)
	})
}

func TestClusterPeerToONTAPAccept(t *testing.T) {
	location := time.FixedZone("UTC+3", 3*60*60)
	expTime := strfmt.DateTime(time.Now().In(location))

	date := (*time.Time)(&expTime)

	expTimeStr := date.Format(time.RFC3339)
	expTimePtr := nillable.ToPointer(expTimeStr)

	t.Run("WithExpiryTime", func(tt *testing.T) {
		params := ClusterPeerCreateParams{
			Name:               "test",
			IPAddresses:        []string{"1.2.3.4"},
			ExpiryTime:         expTimePtr,
			GeneratePassphrase: false,
		}
		otParams := clusterPeerToONTAPAccept(params)
		assert.Equal(tt, params.Name, *otParams.Info.Name)
		assert.Equal(tt, *expTimePtr, *otParams.Info.Authentication.ExpiryTime)

		var ipAddresses []string
		for _, ip := range otParams.Info.Remote.IPAddresses {
			if ip != nil {
				ipAddresses = append(ipAddresses, string(*ip))
			}
		}
		assert.Equal(tt, params.IPAddresses, ipAddresses)
	})
	t.Run("WithoutExpiryTime", func(tt *testing.T) {
		params := ClusterPeerCreateParams{
			Name:               "test",
			IPAddresses:        []string{"1.2.3.4"},
			IPSpace:            "ipSpace",
			GeneratePassphrase: false,
		}

		otParams := clusterPeerToONTAPAccept(params)
		assert.Equal(tt, params.Name, *otParams.Info.Name)
		assert.Nil(tt, otParams.Info.Authentication.ExpiryTime)

		var ipAddresses []string
		for _, ip := range otParams.Info.Remote.IPAddresses {
			if ip != nil {
				ipAddresses = append(ipAddresses, string(*ip))
			}
		}
		assert.Equal(tt, params.IPAddresses, ipAddresses)
	})
}

func TestScheduleCollectionGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := scheduleCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &ScheduleCollectionGetParams{
			BaseParams: BaseParams{Fields: []string{"field1"}},
			Name:       "name",
		}
		otParams := scheduleCollectionGetParamsToONTAP(params)
		assert.Equal(tt, []string{"field1"}, otParams.Fields)
		assert.Equal(tt, "name", *otParams.Name)
	})
}

func TestSvmPeerGetCollectionParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := svmPeerGetCollectionParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SvmPeerGetCollectionParams{
			BaseParams: BaseParams{Fields: []string{"peer", "state"}}, SvmName: nillable.ToPointer("svm-1"), PeerSvmName: nillable.ToPointer("svm-2"),
		}
		otParams := svmPeerGetCollectionParamsToONTAP(params)
		assert.Equal(tt, []string{"peer", "state"}, otParams.Fields)
		assert.Equal(tt, params.SvmName, otParams.SvmName)
		assert.Equal(tt, params.PeerSvmName, otParams.PeerSvmName)
	})
}

func TestSnapmirrorRelationshipListParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipListParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipListParams{
			SourcePath:      "src-path",
			DestinationPath: "dst-path",
		}
		otParams := snapmirrorRelationshipListParamsToONTAP(params)
		assert.Equal(tt, params.SourcePath, *otParams.SourcePath)
		assert.Equal(tt, params.DestinationPath, *otParams.DestinationPath)
	})
}

func TestSnapmirrorRelationshipModifyParamsToONTAP(t *testing.T) {
	t.Run("WithNilParams", func(tt *testing.T) {
		result := snapmirrorRelationshipModifyParamsToONTAP(nil)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.Info)
	})

	t.Run("WithEmptyParams", func(tt *testing.T) {
		params := &SnapmirrorRelationshipModifyParams{}
		result := snapmirrorRelationshipModifyParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.Nil(tt, result.Info.TransferSchedule)
	})

	t.Run("WithTransferSchedule", func(tt *testing.T) {
		params := &SnapmirrorRelationshipModifyParams{
			UUID:             "test-uuid",
			TransferSchedule: strPtr("daily"),
		}
		result := snapmirrorRelationshipModifyParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-uuid", result.UUID)
		assert.NotNil(tt, result.Info.TransferSchedule)
		assert.Equal(tt, "daily", *result.Info.TransferSchedule.Name)
	})
}

func TestSnapmirrorRelationshipListDestinationsParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipListDestinationsParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipListDestinationsParams{
			SourcePath:         nillable.GetStringPtr("src-path"),
			DestinationPath:    nillable.GetStringPtr("dst-path"),
			SourceSVMName:      nillable.GetStringPtr("src-svm"),
			DestinationSVMName: nillable.GetStringPtr("src-svm"),
		}
		otParams := snapmirrorRelationshipListDestinationsParamsToONTAP(params)
		assert.Equal(tt, params.SourcePath, otParams.SourcePath)
		assert.Equal(tt, params.DestinationPath, otParams.DestinationPath)
		assert.Equal(tt, params.SourceSVMName, otParams.SourceSvmName)
		assert.Equal(tt, params.DestinationSVMName, otParams.DestinationSvmName)
	})
}

func TestConvertSnapmirrorRelationshipListFromONTAP(t *testing.T) {
	t.Run("WhenResponseNil", func(tt *testing.T) {
		response := convertSnapmirrorRelationshipListFromREST(nil)
		assert.Nil(tt, response)
	})
	t.Run("WhenPayloadIsNil", func(tt *testing.T) {
		response := convertSnapmirrorRelationshipListFromREST(&snapmirror.SnapmirrorRelationshipsGetOK{Payload: nil})
		assert.Empty(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		response := convertSnapmirrorRelationshipListFromREST(&snapmirror.SnapmirrorRelationshipsGetOK{Payload: &models.SnapmirrorRelationshipResponse{
			Links:      nil,
			NumRecords: nil,
			SnapmirrorRelationshipResponseInlineRecords: []*models.SnapmirrorRelationship{
				{
					State: nil,
				},
			},
		}})
		assert.NotNil(tt, response)
		assert.Equal(tt, 1, len(response))
	})
}

func TestConvertSnapmirrorRelationshipGetFromONTAP(t *testing.T) {
	t.Run("WhenResponseNil", func(tt *testing.T) {
		response := convertSnapmirrorRelationshipGetFromREST(nil)
		assert.Nil(tt, response)
	})
	t.Run("WhenPayloadIsNil", func(tt *testing.T) {
		response := convertSnapmirrorRelationshipGetFromREST(&snapmirror.SnapmirrorRelationshipGetOK{Payload: nil})
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		UUID := strfmt.UUID("testUUID")
		response := convertSnapmirrorRelationshipGetFromREST(&snapmirror.SnapmirrorRelationshipGetOK{Payload: &models.SnapmirrorRelationship{
			UUID: &UUID,
		}})
		assert.NotNil(tt, response)
		assert.Equal(tt, &UUID, response.UUID)
	})
}

func TestSnapmirrorRelationshipGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipGetParams{
			UUID: "theUUID",
		}
		otParams := snapmirrorRelationshipGetParamsToONTAP(params)
		assert.Equal(tt, "theUUID", otParams.UUID)
	})
}

func TestSnapmirrorRelationshipDeleteGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipDeleteParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipDeleteParams{
			UUID: "zeUUID",
		}
		otParams := snapmirrorRelationshipDeleteParamsToONTAP(params)
		assert.Equal(tt, "zeUUID", otParams.UUID)
	})
}

func strPtr(s string) *string {
	return &s
}
