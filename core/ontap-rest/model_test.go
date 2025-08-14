package ontap_rest

import (
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

func TestGcpKmsDeleteParamsToOntap(t *testing.T) {
	t.Run("WhenParamsIsNil", func(tt *testing.T) {
		otParams := gcpKmsDeleteParamsToOntap(nil)
		assert.NotNil(tt, otParams)
		assert.Empty(tt, otParams.UUID)
	})
	t.Run("WhenUUIDInParamsIsSet", func(tt *testing.T) {
		params := GcpKmsDeleteParams{UUID: "uuid1"}
		otParams := gcpKmsDeleteParamsToOntap(&params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "uuid1", otParams.UUID)
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
			Name:            nillable.ToPointer("snapmirror-snapshot"),
		}

		otParams := snapshotCollectionGetParamsToONTAP(params)
		assert.Equal(tt, "field", otParams.Fields[0])
		assert.Equal(tt, "uuid", otParams.VolumeUUID)
		assert.Equal(tt, "lab", *otParams.SnapmirrorLabel)
		assert.Equal(tt, "snapmirror-snapshot", *otParams.Name)
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
	t.Run("WhenQoSPolicyNameSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:       "svm-uuid",
			QoSPolicyName: nillable.ToPointer("test-qos-policy"),
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.NotNil(tt, otParams.Info.QosPolicy)
		assert.Equal(tt, "test-qos-policy", *otParams.Info.QosPolicy.Name)
	})

	t.Run("WhenQoSPolicyNameIsNil", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:       "svm-uuid",
			QoSPolicyName: nil,
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Nil(tt, otParams.Info.QosPolicy)
	})

	t.Run("WhenMultipleFieldsSet", func(tt *testing.T) {
		params := &SvmModifyParams{
			SvmUUID:              "svm-uuid",
			RetentionPeriodHours: nillable.ToPointer(int64(666)),
			QoSPolicyName:        nillable.ToPointer("test-qos-policy"),
		}
		otParams := svmModifyParamsToOntap(params)
		assert.Equal(tt, "svm-uuid", otParams.UUID)
		assert.Equal(tt, int64(666), *otParams.Info.RetentionPeriod)
		assert.NotNil(tt, otParams.Info.QosPolicy)
		assert.Equal(tt, "test-qos-policy", *otParams.Info.QosPolicy.Name)
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

func TestVolumeModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := volumeModifyParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenQuotaEnabledSet_ThenQuotaIsSet", func(tt *testing.T) {
		val := true
		params := &VolumeModifyParams{UUID: "uuid", QuotaEnabled: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Quota)
		assert.Equal(tt, &val, result.Info.Quota.Enabled)
	})

	t.Run("WhenReKeySet_ThenEncryptionIsSet", func(tt *testing.T) {
		val := true
		params := &VolumeModifyParams{UUID: "uuid", ReKey: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Encryption)
		assert.Equal(tt, &val, result.Info.Encryption.Rekey)
	})

	t.Run("WhenSplitInitiatedSet_ThenCloneAndNoReturnTimeout", func(tt *testing.T) {
		val := true
		params := &VolumeModifyParams{UUID: "uuid", SplitInitiated: &val, MatchParentStorageTier: true}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Clone)
		assert.Equal(tt, &val, result.Info.Clone.SplitInitiated)
		assert.NotNil(tt, result.CloneMatchParentStorageTier)
	})

	t.Run("WhenStateSet_ThenStateIsSet", func(tt *testing.T) {
		state := "online"
		params := &VolumeModifyParams{UUID: "uuid", State: &state}
		result := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, &state, result.Info.State)
	})

	t.Run("WhenSnapshotPolicyNameSet_ThenSnapshotPolicyIsSet", func(tt *testing.T) {
		name := "policy"
		params := &VolumeModifyParams{UUID: "uuid", SnapshotPolicyName: &name}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.SnapshotPolicy)
		assert.Equal(tt, &name, result.Info.SnapshotPolicy.Name)
	})

	t.Run("WhenMovementSet_ThenMovementIsSet", func(tt *testing.T) {
		tiering := "auto"
		state := "moving"
		aggUUID := "agg-uuid"
		aggName := "agg-name"
		params := &VolumeModifyParams{
			UUID: "uuid",
			Movement: &VolumeMovementParams{
				TieringPolicy: &tiering,
				State:         &state,
				VolumeMovementDestinationAggregate: &VolumeMovementDestinationAggregate{
					DestinationAggregateUUID: &aggUUID,
					DestinationAggregateName: &aggName,
				},
			},
		}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Movement)
		assert.NotNil(tt, result.Info.Movement.DestinationAggregate)
	})

	t.Run("WhenCommentSet_ThenCommentIsSet", func(tt *testing.T) {
		comment := "test"
		params := &VolumeModifyParams{UUID: "uuid", Comment: &comment}
		result := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, &comment, result.Info.Comment)
	})

	t.Run("WhenSizeAndLogicalSpaceAndSnapReserveAndMaxAutoSizeSet_ThenSpaceIsSet", func(tt *testing.T) {
		size := uint64(100)
		logical := true
		snap := int64(5)
		maxAuto := uint64(200)
		params := &VolumeModifyParams{
			UUID:                    "uuid",
			Size:                    &size,
			LogicalSpaceEnforcement: &logical,
			SnapReserve:             &snap,
			MaxAutoSize:             &maxAuto,
		}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Space)
		assert.NotNil(tt, result.Info.Space.Size)
		assert.NotNil(tt, result.Info.Space.LogicalSpace)
		assert.NotNil(tt, result.Info.Space.Snapshot)
	})

	t.Run("WhenSnapshotDirectoryAccessEnabledSet_ThenFieldIsSet", func(tt *testing.T) {
		val := true
		params := &VolumeModifyParams{UUID: "uuid", SnapshotDirectoryAccessEnabled: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, &val, result.Info.SnapshotDirectoryAccessEnabled)
	})

	t.Run("WhenSetAtTimeEnabledSet_ThenFieldIsSet", func(tt *testing.T) {
		val := true
		params := &VolumeModifyParams{UUID: "uuid", SetAtTimeEnabled: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, &val, result.Info.AccessTimeEnabled)
	})

	t.Run("WhenTieringPolicyAndCoolingDaysSet_ThenTieringIsSet", func(tt *testing.T) {
		policy := "auto"
		days := int64(7)
		params := &VolumeModifyParams{
			UUID: "uuid",
			TieringPolicy: &TieringPolicy{
				CoolAccessTieringPolicy: policy,
				MinCoolingDays:          days,
			},
		}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Tiering)
		assert.Equal(tt, policy, *result.Info.Tiering.Policy)
		assert.Equal(tt, days, *result.Info.Tiering.MinCoolingDays)
	})

	t.Run("WhenTieringPolicyIsNone_ThenCoolingDaysIsNil", func(tt *testing.T) {
		policy := "none"
		days := int64(7)
		params := &VolumeModifyParams{
			UUID: "uuid",
			TieringPolicy: &TieringPolicy{
				CoolAccessTieringPolicy: policy,
				MinCoolingDays:          days,
			},
		}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Tiering)
		assert.Equal(tt, policy, *result.Info.Tiering.Policy)
		assert.Nil(tt, result.Info.Tiering.MinCoolingDays)
	})

	t.Run("WhenCloudRetrievalPolicySet_ThenFieldIsSet", func(tt *testing.T) {
		val := "policy"
		params := &VolumeModifyParams{
			UUID: "uuid",
			TieringPolicy: &TieringPolicy{
				CloudRetrievalPolicy: val,
			},
		}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.CloudRetrievalPolicy)
		assert.Equal(tt, val, *result.Info.CloudRetrievalPolicy)
	})

	t.Run("WhenQosPolicySet_ThenQosIsSet", func(tt *testing.T) {
		val := "qos"
		params := &VolumeModifyParams{UUID: "uuid", QosPolicy: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.Qos)
		assert.NotNil(tt, result.Info.Qos.Policy)
	})

	t.Run("WhenRestoreToSnapshotUUIDSet_ThenFieldIsSet", func(tt *testing.T) {
		val := "snap-uuid"
		params := &VolumeModifyParams{UUID: "uuid", RestoreToSnapshotUUID: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.Equal(tt, val, *result.RestoreToSnapshotUUID)
	})

	t.Run("WhenAntiRansomwareStateSet_ThenFieldIsSet", func(tt *testing.T) {
		val := "enabled"
		params := &VolumeModifyParams{UUID: "uuid", AntiRansomwareState: &val}
		result := volumeModifyParamsToONTAP(params)
		assert.NotNil(tt, result.Info.AntiRansomware)
		assert.Equal(tt, &val, result.Info.AntiRansomware.State)
	})
}

func TestLunModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := lunModifyParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &LunUpdateParams{
			UUID:       "uuid",
			SvmName:    "svm",
			Name:       "lun",
			VolumeName: "vol",
			Size:       1234,
		}
		result := lunModifyParamsToONTAP(params)
		assert.Equal(tt, "uuid", result.UUID)
		assert.NotNil(tt, result.Info)
		assert.NotNil(tt, result.Info.Name)
		assert.NotNil(tt, result.Info.Space)
		assert.Equal(tt, &params.Size, result.Info.Space.Size)
		assert.NotNil(tt, result.ReturnTimeout)
	})
}

func TestNodesGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := nodesGetParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		maxRecords := int64(10)
		returnRecords := true
		params := &NodesGetParams{
			BaseParams: BaseParams{
				Fields:        []string{"field1", "field2"},
				ReturnRecords: &returnRecords,
				MaxRecords:    &maxRecords,
			},
		}
		result := nodesGetParamsToONTAP(params)
		assert.Equal(tt, []string{"field1", "field2"}, result.Fields)
	})
}

func TestNetworkIPInterfacesGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := networkIPInterfacesGetParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenAllFieldsSet_ThenFieldsAreSet", func(tt *testing.T) {
		maxRecords := int64(5)
		fields := []string{"ip.address", "name"}
		svmName := "svm1"
		name := "lif1"
		svmUUID := "uuid1"
		ipAddress := "10.0.0.1"
		servicePolicyName := "default-intercluster"
		params := &NetworkIPInterfacesGetParams{
			BaseParams: BaseParams{
				Fields:     fields,
				MaxRecords: &maxRecords,
			},
			SvmName:           &svmName,
			Name:              &name,
			SvmUUID:           &svmUUID,
			IPAddress:         &ipAddress,
			ServicePolicyName: &servicePolicyName,
		}
		result := networkIPInterfacesGetParamsToONTAP(params)
		assert.Equal(tt, fields, result.Fields)
		assert.Equal(tt, &svmName, result.SvmName)
		assert.Equal(tt, &name, result.Name)
		assert.Equal(tt, &svmUUID, result.SvmUUID)
		assert.Equal(tt, &ipAddress, result.IPAddress)
		assert.Equal(tt, &servicePolicyName, result.ServicePolicyName)
	})
}

func TestVolumeDeleteParamsToONTAPCollectionDelete(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := volumeDeleteParamsToONTAPCollectionDelete(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &VolumeDeleteParams{
			Name: "vol1",
		}
		result := volumeDeleteParamsToONTAPCollectionDelete(params)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.Name)
		assert.Equal(tt, "vol1", *result.Name)
		assert.NotNil(tt, result.Force)
		assert.Equal(tt, "true", *result.Force)
		assert.NotNil(tt, result.ReturnTimeout)
	})
}

func TestVolumeCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &VolumeCreateParams{
			Aggregates:                     []string{"aggr1"},
			Name:                           "vol1",
			Type:                           "rw",
			Size:                           1024,
			Svm:                            "svm1",
			SnapshotReservePercent:         5,
			SnapshotDirectoryAccessEnabled: true,
			TieringPolicy: &TieringPolicy{
				CoolAccessTieringPolicy: "auto",
				MinCoolingDays:          30,
				CloudRetrievalPolicy:    "default",
			},
		}
		result := volumeCreateParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.Info)
		assert.Equal(tt, "vol1", *result.Info.Name)
		assert.Equal(tt, "rw", *result.Info.Type)
		assert.Equal(tt, int64(1024), *result.Info.Size)
		assert.Equal(tt, "svm1", *result.Info.Svm.Name)
		assert.NotNil(tt, result.Info.Space)
		assert.NotNil(tt, result.Info.VolumeInlineAggregates)
		assert.Equal(tt, "true", *result.ReturnRecords)
		assert.Equal(tt, params.TieringPolicy.CoolAccessTieringPolicy, *result.Info.Tiering.Policy)
		assert.Equal(tt, params.TieringPolicy.MinCoolingDays, *result.Info.Tiering.MinCoolingDays)
		assert.Equal(tt, params.TieringPolicy.CloudRetrievalPolicy, *result.Info.CloudRetrievalPolicy)
	})
}

func TestIscsiServiceGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := iscsiServiceGetParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &IscsiGetParams{
			SvmUUID: "uuid1",
			BaseParams: BaseParams{
				Fields: []string{"field1"},
			},
		}
		result := iscsiServiceGetParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.Equal(tt, "uuid1", *result.SvmUUID)
		assert.Equal(tt, []string{"field1"}, result.Fields)
		assert.NotNil(tt, result.ReturnTimeout)
	})
}

func TestIscsiServiceCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		result := iscsiServiceCreateParamsToONTAP(nil)
		assert.NotNil(tt, result)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &IscsiCreateParams{
			SvmUUID: "uuid1",
		}
		result := iscsiServiceCreateParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.Info)
		assert.Equal(tt, "uuid1", *result.Info.Svm.UUID)
	})
}
func TestSnapmirrorRelationshipReleaseParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipReleaseParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipReleaseParams{
			UUID: "test-uuid",
		}
		otParams := snapmirrorRelationshipReleaseParamsToONTAP(params)
		assert.Equal(tt, params.UUID, otParams.UUID)
	})
}

func TestSnapmirrorRelationshipTransferCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipTransferCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipTransferCreateParams{
			UUID: "test-uuid",
		}
		otParams := snapmirrorRelationshipTransferCreateParamsToONTAP(params)
		assert.Equal(tt, params.UUID, otParams.RelationshipUUID)
	})
}

func TestSnapmirrorRelationshipTransferGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipTransferGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipTransferGetParams{
			SnapmirrorUUID: "test-uuid",
			SnapshotName:   "snapshot-1",
		}
		otParams := snapmirrorRelationshipTransferGetParamsToONTAP(params)
		assert.Equal(tt, params.SnapmirrorUUID, otParams.RelationshipUUID)
		assert.Equal(tt, params.SnapshotName, *otParams.Snapshot)
	})
}

func TestCloudTargetCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cloudTargetCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CloudTargetCreateParams{
			Name:      nillable.ToPointer("cloud-target"),
			Container: nillable.ToPointer("cloud-container"),
		}
		otParams := cloudTargetCreateParamsToONTAP(params)
		assert.Equal(tt, *params.Name, *otParams.Info.Name)
		assert.Equal(tt, *params.Container, *otParams.Info.Container)
		assert.Equal(tt, objStoreOwner, *otParams.Info.Owner)
		assert.Equal(tt, objStoreSnapmirrorUse, *otParams.Info.SnapmirrorUse)
		assert.Equal(tt, objStoreProviderType, *otParams.Info.ProviderType)
		assert.Equal(tt, objStoreSnapmirrorUse, *otParams.Info.SnapmirrorUse)
	})
}

func TestCloudTargetCollectionGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cloudTargetCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CloudTargetCollectionGetParams{
			Name: nillable.ToPointer("cloud-target"),
		}
		otParams := cloudTargetCollectionGetParamsToONTAP(params)
		assert.Equal(tt, *params.Name, *otParams.Name)
	})
}
func TestSnapmirrorRelationshipReleaseParamsToONTAP2(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipReleaseParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipReleaseParams{
			UUID: "test-uuid",
		}
		otParams := snapmirrorRelationshipReleaseParamsToONTAP(params)
		assert.Equal(tt, params.UUID, otParams.UUID)
	})
}

func TestSnapmirrorRelationshipTransferCreateParamsToONTAP2(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipTransferCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipTransferCreateParams{
			UUID: "test-uuid",
		}
		otParams := snapmirrorRelationshipTransferCreateParamsToONTAP(params)
		assert.Equal(tt, params.UUID, otParams.RelationshipUUID)
	})
}

func TestSnapmirrorRelationshipTransferGetParamsToONTAP2(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := snapmirrorRelationshipTransferGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SnapmirrorRelationshipTransferGetParams{
			SnapmirrorUUID: "test-uuid",
			SnapshotName:   "snapshot-1",
		}
		otParams := snapmirrorRelationshipTransferGetParamsToONTAP(params)
		assert.Equal(tt, params.SnapmirrorUUID, otParams.RelationshipUUID)
		assert.Equal(tt, params.SnapshotName, *otParams.Snapshot)
	})
}

func TestCloudTargetCreateParamsToONTAP2(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cloudTargetCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CloudTargetCreateParams{
			Name:      nillable.ToPointer("cloud-target"),
			Container: nillable.ToPointer("cloud-container"),
		}
		otParams := cloudTargetCreateParamsToONTAP(params)
		assert.Equal(tt, *params.Name, *otParams.Info.Name)
		assert.Equal(tt, *params.Container, *otParams.Info.Container)
	})
}

func TestCloudTargetCollectionGetParamsToONTAP2(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cloudTargetCollectionGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})

	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CloudTargetCollectionGetParams{
			Name: nillable.ToPointer("cloud-target"),
		}
		otParams := cloudTargetCollectionGetParamsToONTAP(params)
		assert.Equal(tt, *params.Name, *otParams.Name)
	})
}

// Test for dnsCreateParamsToONTAP
func TestDnsCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(t *testing.T) {
		result := dnsCreateParamsToONTAP(nil)
		assert.NotNil(t, result)
	})

	t.Run("WhenParamsSet", func(t *testing.T) {
		domains := []string{"example.com", "test.com"}
		servers := []string{"8.8.8.8", "8.8.4.4"}
		params := &DNSCreateParams{
			Domains:    domains,
			DNSServers: servers,
		}
		result := dnsCreateParamsToONTAP(params)
		assert.NotNil(t, result)
	})
}

func TestExportPolicyCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := exportPolicyCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &ExportPolicyCreateParams{
			Name:    "test-policy",
			SvmName: "test-svm",
			Rules: []*ExportRule{
				{
					ClientMatch:   "10.0.0.8",
					ReadOnlyRule:  "any",
					ReadWriteRule: "any",
					SuperUserRule: "any",
					AnonymousUser: "65534",
					Index:         1,
					Protocols:     []string{"nfs3", "nfs4"},
				},
			},
		}

		otParams := exportPolicyCreateParamsToONTAP(params)
		assert.Equal(tt, "test-policy", *otParams.Info.Name)
		assert.Equal(tt, "test-svm", *otParams.Info.Svm.Name)
		assert.Len(tt, otParams.Info.ExportPolicyInlineRules, 1)
		assert.Equal(tt, "10.0.0.8", *otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineClients[0].Match)
		assert.Equal(tt, "any", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineRoRule[0]))
		assert.Equal(tt, "any", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineRwRule[0]))
		assert.Equal(tt, "any", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineSuperuser[0]))
		assert.Equal(tt, "65534", *otParams.Info.ExportPolicyInlineRules[0].AnonymousUser)
		assert.Equal(tt, int64(1), *otParams.Info.ExportPolicyInlineRules[0].Index)
		assert.Len(tt, otParams.Info.ExportPolicyInlineRules[0].Protocols, 2)
		assert.Equal(tt, "nfs3", *otParams.Info.ExportPolicyInlineRules[0].Protocols[0])
		assert.Equal(tt, "nfs4", *otParams.Info.ExportPolicyInlineRules[0].Protocols[1])
	})
}

func TestExportPolicyGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := exportPolicyGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &ExportPolicyGetParams{
			BaseParams: BaseParams{
				Fields:     []string{"name", "svm"},
				MaxRecords: nillable.ToPointer(int64(0)),
			},
			Name:    nillable.ToPointer("test-policy"),
			SvmName: nillable.ToPointer("test-svm"),
		}

		otParams := exportPolicyGetParamsToONTAP(params)
		assert.Equal(tt, "test-policy", *otParams.Name)
		assert.Equal(tt, "test-svm", *otParams.SvmName)
		assert.Equal(tt, []string{"name", "svm"}, otParams.Fields)
		assert.Equal(tt, "0", *otParams.MaxRecords)
	})
}

func TestExportPolicyModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := exportPolicyModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &ExportPolicyModifyParams{
			BaseParams: BaseParams{},
			ID:         123,
			Name:       nillable.ToPointer("modified-policy"),
			SvmName:    "test-svm",
			Rules: []*ExportRule{
				{
					ClientMatch:   "192.168.0.16",
					ReadOnlyRule:  "none",
					ReadWriteRule: "none",
					SuperUserRule: "none",
					AnonymousUser: "65534",
					Index:         1,
					Protocols:     []string{"nfs3"},
				},
			},
		}

		otParams := exportPolicyModifyParamsToONTAP(params)
		assert.Equal(tt, int64(123), params.ID)
		assert.Equal(tt, "modified-policy", *otParams.Info.Name)
		assert.Len(tt, otParams.Info.ExportPolicyInlineRules, 1)
		assert.Equal(tt, "192.168.0.16", *otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineClients[0].Match)
		assert.Equal(tt, "none", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineRoRule[0]))
		assert.Equal(tt, "none", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineRwRule[0]))
		assert.Equal(tt, "none", string(*otParams.Info.ExportPolicyInlineRules[0].ExportRulesInlineSuperuser[0]))
		assert.Equal(tt, "65534", *otParams.Info.ExportPolicyInlineRules[0].AnonymousUser)
		assert.Len(tt, otParams.Info.ExportPolicyInlineRules[0].Protocols, 1)
		assert.Equal(tt, "nfs3", *otParams.Info.ExportPolicyInlineRules[0].Protocols[0])
	})
}

func TestExportPolicyDeleteParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := exportPolicyDeleteParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &ExportPolicyDeleteParams{
			BaseParams: BaseParams{},
			Name:       "test-policy",
			SvmName:    "test-svm",
		}

		otParams := exportPolicyDeleteParamsToONTAP(params)
		assert.Equal(tt, "test-policy", *otParams.Name)
		assert.Equal(tt, "test-svm", *otParams.SvmName)
	})
}

func TestNfsServiceGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := nfsServiceGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &NfsServiceGetParams{
			BaseParams: BaseParams{
				Fields: []string{"enabled", "protocol"},
			},
			SvmUUID: "test-svm-uuid",
		}

		otParams := nfsServiceGetParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", otParams.SvmUUID)
		assert.Equal(tt, []string{"enabled", "protocol"}, otParams.Fields)
	})
}

func TestNfsServiceCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := nfsServiceCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &NfsServiceCreateParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Enabled:    nillable.ToPointer(true),
			V3:         nillable.ToPointer(true),
			V4:         nillable.ToPointer(false),
			V41:        nillable.ToPointer(true),
		}

		otParams := nfsServiceCreateParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", *otParams.Info.Svm.UUID)
		assert.True(tt, *otParams.Info.Enabled)
		assert.True(tt, *otParams.Info.Protocol.V3Enabled)
		assert.False(tt, *otParams.Info.Protocol.V40Enabled)
		assert.True(tt, *otParams.Info.Protocol.V41Enabled)
	})
	t.Run("WhenProtocolNotSet", func(tt *testing.T) {
		params := &NfsServiceCreateParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Enabled:    nillable.ToPointer(true),
		}

		otParams := nfsServiceCreateParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", *otParams.Info.Svm.UUID)
		assert.True(tt, *otParams.Info.Enabled)
		assert.Nil(tt, otParams.Info.Protocol)
	})
}

func TestNfsServiceModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := nfsServiceModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &NfsServiceModifyParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Enabled:    nillable.ToPointer(false),
			V3:         nillable.ToPointer(false),
			V4:         nillable.ToPointer(true),
			V41:        nillable.ToPointer(false),
		}

		otParams := nfsServiceModifyParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", otParams.SvmUUID)
		assert.False(tt, *otParams.Info.Enabled)
		assert.False(tt, *otParams.Info.Protocol.V3Enabled)
		assert.True(tt, *otParams.Info.Protocol.V40Enabled)
		assert.False(tt, *otParams.Info.Protocol.V41Enabled)
	})
	t.Run("WhenProtocolNotSet", func(tt *testing.T) {
		params := &NfsServiceModifyParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Enabled:    nillable.ToPointer(true),
		}

		otParams := nfsServiceModifyParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", otParams.SvmUUID)
		assert.True(tt, *otParams.Info.Enabled)
		assert.Nil(tt, otParams.Info.Protocol)
	})
}

func TestCifsServiceGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cifsServiceGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CifsServiceGetParams{
			BaseParams: BaseParams{
				Fields: []string{"name", "enabled"},
			},
			SvmName: nillable.ToPointer("test-svm"),
			SvmUUID: nillable.ToPointer("test-svm-uuid"),
		}

		otParams := cifsServiceGetParamsToONTAP(params)
		assert.Equal(tt, "test-svm", *otParams.SvmName)
		assert.Equal(tt, "test-svm-uuid", *otParams.SvmUUID)
		assert.Equal(tt, []string{"name", "enabled"}, otParams.Fields)
	})
}

func TestCifsServiceCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cifsServiceCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CifsServiceCreateParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Name:       "test-cifs",
			Enabled:    nillable.ToPointer(true),
			AdDomain:   nillable.ToPointer("test.domain.com"),
		}

		otParams := cifsServiceCreateParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", *otParams.Info.Svm.UUID)
		assert.Equal(tt, "test-cifs", *otParams.Info.Name)
		assert.True(tt, *otParams.Info.Enabled)
		assert.Equal(tt, "test.domain.com", *otParams.Info.AdDomain.Fqdn)
	})
}

func TestCifsServiceModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := cifsServiceModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &CifsServiceModifyParams{
			BaseParams: BaseParams{},
			SvmUUID:    "test-svm-uuid",
			Enabled:    nillable.ToPointer(false),
		}

		otParams := cifsServiceModifyParamsToONTAP(params)
		assert.Equal(tt, "test-svm-uuid", otParams.SvmUUID)
		assert.False(tt, *otParams.Info.Enabled)
	})
}

func TestSnapmirrorCloudSnapshotGetParamsToONTAP(t *testing.T) {
	t.Run("WhenAllParamsEmpty", func(tt *testing.T) {
		params := &SnapmirrorCloudSnapshotGetParams{}
		otParams := snapmirrorCloudSnapshotGetParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Empty(tt, otParams.ObjectStoreUUID)
		assert.Empty(tt, otParams.EndpointUUID)
		assert.Empty(tt, otParams.UUID)
	})

	t.Run("WhenAllParamsSet", func(tt *testing.T) {
		params := &SnapmirrorCloudSnapshotGetParams{
			ObjectStoreUUID: "obj-store-uuid",
			EndpointUUID:    "endpoint-uuid",
			SnapshotUUID:    "snapshot-uuid",
		}
		otParams := snapmirrorCloudSnapshotGetParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "obj-store-uuid", otParams.ObjectStoreUUID)
		assert.Equal(tt, "endpoint-uuid", otParams.EndpointUUID)
		assert.Equal(tt, "snapshot-uuid", otParams.UUID)
	})

	t.Run("WhenOnlyObjectStoreUUIDSet", func(tt *testing.T) {
		params := &SnapmirrorCloudSnapshotGetParams{
			ObjectStoreUUID: "obj-store-uuid",
		}
		otParams := snapmirrorCloudSnapshotGetParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "obj-store-uuid", otParams.ObjectStoreUUID)
		assert.Empty(tt, otParams.EndpointUUID)
		assert.Empty(tt, otParams.UUID)
	})

	t.Run("WhenOnlyEndpointUUIDSet", func(tt *testing.T) {
		params := &SnapmirrorCloudSnapshotGetParams{
			EndpointUUID: "endpoint-uuid",
		}
		otParams := snapmirrorCloudSnapshotGetParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Empty(tt, otParams.ObjectStoreUUID)
		assert.Equal(tt, "endpoint-uuid", otParams.EndpointUUID)
		assert.Empty(tt, otParams.UUID)
	})

	t.Run("WhenOnlySnapshotUUIDSet", func(tt *testing.T) {
		params := &SnapmirrorCloudSnapshotGetParams{
			SnapshotUUID: "snapshot-uuid",
		}
		otParams := snapmirrorCloudSnapshotGetParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Empty(tt, otParams.ObjectStoreUUID)
		assert.Empty(tt, otParams.EndpointUUID)
		assert.Equal(tt, "snapshot-uuid", otParams.UUID)
	})
}

func TestSecurityAuditModifyParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := securityAuditModifyParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SecurityAuditUpdateParams{
			Cli:    true,
			HTTP:   true,
			Ontapi: true,
		}

		otParams := securityAuditModifyParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info)
		assert.NotNil(tt, otParams.Info.Cli)
		assert.True(tt, *otParams.Info.Cli)
		assert.NotNil(tt, otParams.Info.HTTP)
		assert.True(tt, *otParams.Info.HTTP)
		assert.NotNil(tt, otParams.Info.Ontapi)
		assert.True(tt, *otParams.Info.Ontapi)
	})
}

func TestSecurityLogForwardingGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := securityLogForwardingGetParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SecurityLogForwardingGetParams{
			Address: "test-address",
			Port:    int64(1234),
		}

		otParams := securityLogForwardingGetParamsToONTAP(params)
		assert.Equal(tt, "test-address", otParams.Address)
		assert.Equal(tt, int64(1234), otParams.Port)
	})
}

func TestSecurityLogForwardingCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil", func(tt *testing.T) {
		otParams := securityLogForwardingCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
	})
	t.Run("WhenParamsSet", func(tt *testing.T) {
		params := &SecurityLogForwardingCreateParams{
			BaseParams:   BaseParams{},
			Address:      nillable.ToPointer("test-address"),
			Port:         nillable.ToPointer(int64(1234)),
			Protocol:     nillable.ToPointer("http"),
			Facility:     nillable.ToPointer("test-facility"),
			VerifyServer: nillable.ToPointer(true),
		}

		otParams := securityLogForwardingCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info)
		assert.NotNil(tt, otParams.Info.Address)
		assert.Equal(tt, "test-address", *otParams.Info.Address)
		assert.NotNil(tt, otParams.Info.Port)
		assert.Equal(tt, int64(1234), *otParams.Info.Port)
		assert.NotNil(tt, otParams.Info.Protocol)
		assert.Equal(tt, "http", *otParams.Info.Protocol)
		assert.NotNil(tt, otParams.Info.Facility)
		assert.Equal(tt, "test-facility", *otParams.Info.Facility)
		assert.NotNil(tt, otParams.Info.VerifyServer)
		assert.True(tt, *otParams.Info.VerifyServer)
	})
}

func TestLunCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		otParams := lunCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
		assert.Nil(tt, otParams.Info)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		thinProvisioning := true
		params := &LunCreateParams{
			SvmName:                        "test-svm",
			Name:                           "test-lun",
			OsType:                         "LINUX",
			VolumeName:                     "test-volume",
			Size:                           1073741824, // 1GB
			ThinProvisioningSupportEnabled: &thinProvisioning,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info)

		// Verify SVM
		assert.NotNil(tt, otParams.Info.Svm)
		assert.Equal(tt, "test-svm", *otParams.Info.Svm.Name)

		// Verify Location
		assert.NotNil(tt, otParams.Info.Location)
		assert.NotNil(tt, otParams.Info.Location.Volume)
		assert.Equal(tt, "test-volume", *otParams.Info.Location.Volume.Name)

		// Verify Space
		assert.NotNil(tt, otParams.Info.Space)
		assert.Equal(tt, int64(1073741824), *otParams.Info.Space.Size)
		assert.Equal(tt, &thinProvisioning, otParams.Info.Space.ScsiThinProvisioningSupportEnabled)

		// Verify OS Type mapping
		assert.Equal(tt, "LINUX", *otParams.Info.OsType)
	})

	t.Run("WhenOsTypeIsESXI_ThenMapsToVMWARE", func(tt *testing.T) {
		params := &LunCreateParams{
			SvmName:    "test-svm",
			Name:       "test-lun",
			OsType:     "ESXI",
			VolumeName: "test-volume",
			Size:       1073741824,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "VMWARE", *otParams.Info.OsType)
	})

	t.Run("WhenOsTypeIsWindows_ThenMapsToWINDOWS", func(tt *testing.T) {
		params := &LunCreateParams{
			SvmName:    "test-svm",
			Name:       "test-lun",
			OsType:     "WINDOWS",
			VolumeName: "test-volume",
			Size:       1073741824,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "WINDOWS", *otParams.Info.OsType)
	})

	t.Run("WhenOsTypeIsUnknown_ThenOsTypeNotSet", func(tt *testing.T) {
		params := &LunCreateParams{
			SvmName:    "test-svm",
			Name:       "test-lun",
			OsType:     "UNKNOWN",
			VolumeName: "test-volume",
			Size:       1073741824,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "UNKNOWN", *otParams.Info.OsType)
	})

	t.Run("WhenThinProvisioningIsNil_ThenFieldIsNil", func(tt *testing.T) {
		params := &LunCreateParams{
			SvmName:                        "test-svm",
			Name:                           "test-lun",
			OsType:                         "LINUX",
			VolumeName:                     "test-volume",
			Size:                           1073741824,
			ThinProvisioningSupportEnabled: nil,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info.Space)
		assert.Nil(tt, otParams.Info.Space.ScsiThinProvisioningSupportEnabled)
	})

	t.Run("WhenThinProvisioningIsFalse_ThenFieldIsFalse", func(tt *testing.T) {
		thinProvisioning := false
		params := &LunCreateParams{
			SvmName:                        "test-svm",
			Name:                           "test-lun",
			OsType:                         "LINUX",
			VolumeName:                     "test-volume",
			Size:                           1073741824,
			ThinProvisioningSupportEnabled: &thinProvisioning,
		}

		otParams := lunCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info.Space)
		assert.Equal(tt, &thinProvisioning, otParams.Info.Space.ScsiThinProvisioningSupportEnabled)
	})
}

func TestIgroupCreateParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsNil_ThenReturnsDefault", func(tt *testing.T) {
		otParams := igroupCreateParamsToONTAP(nil)
		assert.NotNil(tt, otParams)
		assert.Nil(tt, otParams.Info)
	})

	t.Run("WhenParamsSet_ThenFieldsAreSet", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: []string{"iqn.1993-08.org.debian:01:c9a5ccf7ca95", "iqn.1993-08.org.debian:01:d0b6dde8db06"},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info)

		// Verify SVM
		assert.NotNil(tt, otParams.Info.Svm)
		assert.Equal(tt, "test-svm", *otParams.Info.Svm.Name)

		// Verify basic fields
		assert.Equal(tt, "test-igroup", *otParams.Info.Name)
		assert.Equal(tt, "LINUX", *otParams.Info.OsType)
		assert.Equal(tt, "test-job-123", *otParams.Info.Comment)
		assert.Equal(tt, models.IgroupProtocolIscsi, *otParams.Info.Protocol)

		// Verify initiators
		assert.Len(tt, otParams.Info.IgroupInlineInitiators, 2)
		assert.Equal(tt, "iqn.1993-08.org.debian:01:c9a5ccf7ca95", *otParams.Info.IgroupInlineInitiators[0].Name)
		assert.Equal(tt, "iqn.1993-08.org.debian:01:d0b6dde8db06", *otParams.Info.IgroupInlineInitiators[1].Name)
	})

	t.Run("WhenOsTypeIsESXI_ThenMapsToVMWARE", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "ESXI",
			Initiators: []string{"iqn.1998-01.com.vmware:esx001-0badb9cf"},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "VMWARE", *otParams.Info.OsType)
	})

	t.Run("WhenOsTypeIsLinux_ThenMapsToLINUX", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: []string{"iqn.1994-05.com.example:test1"},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "LINUX", *otParams.Info.OsType)
	})

	t.Run("WhenOsTypeIsWindows_ThenMapsToWINDOWS", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "WINDOWS",
			Initiators: []string{"iqn.1991-05.com.microsoft:wlm-sql-gcnv1-sql-server"},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "WINDOWS", *otParams.Info.OsType)
	})

	t.Run("WhenInitiatorsEmpty_ThenInitiatorsArrayIsEmpty", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: []string{},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info.IgroupInlineInitiators)
		assert.Len(tt, otParams.Info.IgroupInlineInitiators, 0)
	})

	t.Run("WhenInitiatorsNil_ThenInitiatorsArrayIsEmpty", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: nil,
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.NotNil(tt, otParams.Info.IgroupInlineInitiators)
		assert.Len(tt, otParams.Info.IgroupInlineInitiators, 0)
	})

	t.Run("WhenSingleInitiator_ThenInitiatorsArrayHasOne", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: []string{"iqn.1993-08.org.debian:01:c9a5ccf7ca95"},
			JobID:      "test-job-123",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Len(tt, otParams.Info.IgroupInlineInitiators, 1)
		assert.Equal(tt, "iqn.1993-08.org.debian:01:c9a5ccf7ca95", *otParams.Info.IgroupInlineInitiators[0].Name)
	})

	t.Run("WhenJobIDEmpty_ThenCommentIsEmpty", func(tt *testing.T) {
		params := &IgroupCreateParams{
			SvmName:    "test-svm",
			Name:       "test-igroup",
			OsType:     "LINUX",
			Initiators: []string{"iqn.1993-08.org.debian:01:c9a5ccf7ca95"},
			JobID:      "",
		}

		otParams := igroupCreateParamsToONTAP(params)
		assert.NotNil(tt, otParams)
		assert.Equal(tt, "", *otParams.Info.Comment)
	})
}
