//go:build !test_exclude

package sanity

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

var (
	globalLargePoolName          = "lp" + strconv.FormatInt(time.Now().Unix(), 10)
	globalLargeVolumeName        = "lv" + strconv.FormatInt(time.Now().Unix(), 10)
	globalLargeCapacityPoolUUID  = ""
	globalLargeVolumeUUID        = ""
	secondaryZone                = getEnvOrDefault("SECONDARY_ZONE", "australia-southeast1-c")
	defaultLargeCapacityPoolSize = 14293651161088 // 13 TiB
	defaultLargeVolumeSize       = 13194139533312 // 12 TiB
)

func TestCreateLargeCapacityPoolAndWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	params := googleproxyclient.V1betaCreatePoolParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
	}
	poolReq := &googleproxyclient.PoolV1beta{
		ResourceId:               globalLargePoolName,
		ServiceLevel:             googleproxyclient.PoolV1betaServiceLevelFLEX,
		SizeInBytes:              float64(defaultLargeCapacityPoolSize),
		Network:                  globalNetwork,
		UnifiedPool:              googleproxyclient.OptBool{Value: true, Set: true},
		Zone:                     googleproxyclient.OptString{Value: globalLocationId, Set: true},
		CustomPerformanceEnabled: googleproxyclient.OptBool{Value: true, Set: true},
		TotalThroughputMibps:     googleproxyclient.OptNilFloat64{Value: 64, Set: true},
		TotalIops:                googleproxyclient.OptNilFloat64{Value: 1024, Set: true},
		LargeCapacity:            googleproxyclient.NewOptBool(true),
		SecondaryZone:            googleproxyclient.NewOptString(secondaryZone),
	}

	res, err := client.V1betaCreatePool(ctx, poolReq, params)
	log.Printf("CreatePool response: %+v, err: %v", res, err)
	require.NoError(t, err)

	operation, ok := res.(*googleproxyclient.OperationV1beta)
	require.True(t, ok, "expected OperationV1beta, got %T", res)
	operationID := extractOperationID(operation.GetName().Value)
	require.NotEmpty(t, operationID)

	var pool gcpgenserver.PoolV1beta
	err = json.Unmarshal(operation.GetResponse(), &pool)
	if err != nil {
		log.Printf("Error unmarshalling PoolV1beta: %v", err)
		require.Fail(t, "Failed to unmarshal PoolV1beta")
		return
	}

	globalLargeCapacityPoolUUID = pool.PoolId.Value

	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		OperationId:   operationID,
	}

	done := pollOperationDone(t, client, ctx, describeParams, 100, 30*time.Second)
	require.True(t, done, "operation did not complete in time")
}

func TestGetLargeCapacityPool(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	params := googleproxyclient.V1betaDescribePoolParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
		PoolId:        globalLargeCapacityPoolUUID,
	}

	res, err := client.V1betaDescribePool(ctx, params)
	require.NoError(t, err)

	pool, ok := res.(*googleproxyclient.PoolV1beta)
	require.True(t, ok, "expected PoolV1beta, got %T", res)
	log.Printf("Type of res: %T\n", pool)

	require.Equal(t, googleproxyclient.PoolV1betaStoragePoolStateREADY, pool.StoragePoolState.Value)
	require.Equal(t, "Available for use", pool.StoragePoolStateDetails.Value)
}

func TestCreateLargeVolumeAndWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	params := googleproxyclient.V1betaCreateVolumeParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
	}
	volumeReq := &googleproxyclient.VolumeCreateV1beta{
		Volume: googleproxyclient.VolumeV1beta{
			ResourceId:    globalLargeVolumeName,
			CreationToken: googleproxyclient.NewOptString(globalLargeVolumeName),
			PoolId:        googleproxyclient.NilString{Value: globalLargeCapacityPoolUUID, Null: false},
			QuotaInBytes:  googleproxyclient.OptFloat64{Value: float64(defaultLargeVolumeSize), Set: true},
			Description:   googleproxyclient.OptNilString{Value: "test volume", Set: true},
			Network:       googleproxyclient.OptString{Value: globalNetwork, Set: true},
			Protocols:     []googleproxyclient.ProtocolsV1beta{googleproxyclient.ProtocolsV1betaNFSV4},
			LargeCapacity: googleproxyclient.NewOptNilBool(true),
			StorageClass: googleproxyclient.OptStorageClassV1beta{
				Value: googleproxyclient.StorageClassV1betaSOFTWARE,
				Set:   true,
			},
			LargeVolumeConstituentCount: googleproxyclient.NewOptNilInt32(int32(20)),
			ExportPolicy: googleproxyclient.NewOptExportPolicyV1beta(googleproxyclient.ExportPolicyV1beta{
				Rules: []googleproxyclient.SimpleExportPolicyRuleV1beta{
					{
						AllowedClients: "0.0.0.0/0",
						HasRootAccess:  googleproxyclient.NewOptNilSimpleExportPolicyRuleV1betaHasRootAccess(googleproxyclient.SimpleExportPolicyRuleV1betaHasRootAccessTrue),
						AccessType:     googleproxyclient.SimpleExportPolicyRuleV1betaAccessTypeREADWRITE,
						Nfsv3:          googleproxyclient.NewOptNilBool(true),
						Nfsv4:          googleproxyclient.NewOptNilBool(true),
					},
				},
			}),
		},
	}

	res, err := client.V1betaCreateVolume(ctx, volumeReq, params)
	log.Printf("CreateVolume response: %+v, err: %v", res, err)
	require.NoError(t, err)
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	require.True(t, ok, "expected OperationV1beta, got %T", res)
	operationID := extractOperationID(operation.GetName().Value)

	var volumeV1beta gcpgenserver.VolumeV1beta
	err = json.Unmarshal(operation.GetResponse(), &volumeV1beta)
	if err != nil {
		log.Printf("Error unmarshalling VolumeV1beta: %v", err)
		require.Fail(t, "Failed to unmarshal VolumeV1beta")
		return
	}
	globalLargeVolumeUUID = volumeV1beta.VolumeId.Value

	require.NotEmpty(t, operationID)

	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		OperationId:   operationID,
	}
	done := pollOperationDone(t, client, ctx, describeParams, 20, 30*time.Second)

	require.True(t, done, "operation did not complete in time")
}

func TestGetLargeVolume(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	params := googleproxyclient.V1betaDescribeVolumeParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
		VolumeId:      globalLargeVolumeUUID,
	}

	res, err := client.V1betaDescribeVolume(ctx, params)
	log.Printf("GetVolume response: %+v, err: %v", res, err)
	require.NoError(t, err)
	require.NotNil(t, res)

	volume, ok := res.(*googleproxyclient.VolumeV1beta)
	require.True(t, ok, "expected VolumeV1beta, got %T", res)
	require.True(t, volume.LargeCapacity.Value)
	require.Equal(t, globalLargeVolumeUUID, volume.VolumeId.Value)
	require.Equal(t, globalLargeVolumeName, volume.ResourceId)
	require.Equal(t, googleproxyclient.VolumeV1betaVolumeStateREADY, volume.VolumeState.Value)
	require.Equal(t, googleproxyclient.StorageClassV1betaSOFTWARE, volume.StorageClass.Value)
	require.Equal(t, googleproxyclient.VolumeV1betaEncryptionTypeSERVICEMANAGED, volume.EncryptionType.Value)
	require.Equal(t, googleproxyclient.VolumeV1betaServiceLevelFLEX, volume.ServiceLevel.Value)
	require.Equal(t, "Available for use", volume.VolumeStateDetails.Value)
	require.Equal(t, float64(defaultLargeVolumeSize), volume.QuotaInBytes.Value)
	require.NotEmpty(t, volume.Network.Value)
	require.NotEmpty(t, volume.MountPoints)
}

func TestUpdateLargeVolumeAndWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	time.Sleep(5) // wait a bit before updating the volume
	params := googleproxyclient.V1betaUpdateVolumeParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
		VolumeId:      globalLargeVolumeUUID,
	}
	volumeReq := &googleproxyclient.VolumeUpdateV1beta{
		PoolId:       googleproxyclient.OptNilString{Value: globalLargeCapacityPoolUUID, Null: false},
		QuotaInBytes: googleproxyclient.OptNilFloat64{Value: float64(defaultLargeCapacityPoolSize), Set: true}, // increase to 13 TiB
		Description:  googleproxyclient.OptNilString{Value: "updated volume", Set: true},
	}

	res, err := client.V1betaUpdateVolume(ctx, volumeReq, params)
	log.Printf("UpdateVolume response: %+v, err: %v", res, err)
	require.NoError(t, err)
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	require.True(t, ok, "expected OperationV1beta, got %T", res)
	operationID := extractOperationID(operation.GetName().Value)
	require.NotEmpty(t, operationID)
	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		OperationId:   operationID,
	}
	done := pollOperationDone(t, client, ctx, describeParams, 20, 30*time.Second)
	require.True(t, done, "operation did not complete in time")
}

func TestDeleteLargeVolumeAndWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)

	params := googleproxyclient.V1betaDeleteVolumeParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
		VolumeId:      globalLargeVolumeUUID,
	}

	res, err := client.V1betaDeleteVolume(ctx, googleproxyclient.OptV1betaDeleteVolumeReq{}, params)
	log.Printf("DeleteVolume response: %+v, err: %v", res, err)
	require.NoError(t, err)
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	require.True(t, ok, "expected OperationV1beta, got %T", res)
	operationID := extractOperationID(operation.GetName().Value)
	require.NotEmpty(t, operationID)
	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		OperationId:   operationID,
	}
	done := pollOperationDone(t, client, ctx, describeParams, 20, 30*time.Second)
	require.True(t, done, "operation did not complete in time")
}

func TestDeleteLargeCapacityPoolAndWaitForCompletion(t *testing.T) {
	ctx := context.Background()
	client := getTestClient(t)
	time.Sleep(5) // wait a bit before deleting the pool

	params := googleproxyclient.V1betaDeletePoolParams{
		ProjectNumber: globalProjectNumber,
		LocationId:    globalLocationId,
		PoolId:        globalLargeCapacityPoolUUID,
	}

	res, err := client.V1betaDeletePool(ctx, params)
	log.Printf("DeletePool response: %+v, err: %v", res, err)
	require.NoError(t, err)
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	require.True(t, ok, "expected OperationV1beta, got %T", res)
	operationID := extractOperationID(operation.GetName().Value)
	require.NotEmpty(t, operationID)
	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		ProjectNumber: params.ProjectNumber,
		LocationId:    params.LocationId,
		OperationId:   operationID,
	}
	done := pollOperationDone(t, client, ctx, describeParams, 30, 30*time.Second)
	require.True(t, done, "operation did not complete in time")
}
