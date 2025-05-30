package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestRootVolumeGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, err := client.RootVolumeGet(&VolumeGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenResponseIsEmpty_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, err := client.RootVolumeGet(&VolumeGetParams{})
		assert.EqualError(tt, err, "Root volume for SVM not found")
		assert.Nil(tt, response)
	})

	t.Run("WhenSuccessful_ThenReturnRootVolume", func(tt *testing.T) {
		volumeName := "root-volume"
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{
					{Name: &volumeName},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, err := client.RootVolumeGet(&VolumeGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, volumeName, *response.Name)
	})
}

func TestVolumeDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeDelete(&VolumeDeleteParams{UUID: "someUUID"})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenUuidAndNameAreEmpty_ThenThrowError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeDeleteCollectionOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeDelete(&VolumeDeleteParams{})
		assert.Error(tt, err)
		assert.EqualError(tt, err, "no name filter provided for VolumeDeleteCollection")
	})

	t.Run("WhenVolumeUUIDIsPassed_ThenSuccessfullyDeleteVolume", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeDeleteOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeDelete(&VolumeDeleteParams{UUID: "someUUID"})
		assert.NoError(tt, err)
	})

	t.Run("WhenVolumeNameIsPassed_ThenSuccessfullyDeleteVolume", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeDeleteCollectionOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeDelete(&VolumeDeleteParams{Name: "volumeName"})
		assert.NoError(tt, err)
	})
}

func TestVolumeCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasNoVolumeInfo_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeCreateCreated{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating volume - received no volume info")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasMultipleVolumes_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		volumeName1 := "volume1"
		volumeName2 := "volume2"
		transport := &mockTransport{response: &storage.VolumeCreateCreated{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{
					{Name: &volumeName1},
					{Name: &volumeName2},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating volume - did not receive exactly one volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenSuccessfulWithCreatedResponse_ThenReturnVolume", func(tt *testing.T) {
		volumeName := "test-volume"
		transport := &mockTransport{response: &storage.VolumeCreateCreated{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{{Name: &volumeName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Nil(tt, job)
		assert.Equal(tt, volumeName, *response.Name)
	})

	t.Run("WhenSuccessfulWithAcceptedResponse_ThenReturnVolumeAndJob", func(tt *testing.T) {
		volumeName := "test-volume"
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.VolumeCreateAccepted{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{{Name: &volumeName}},
				Job:     &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, volumeName, *response.Name)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenEmptyRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeCreateAccepted{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating volume - received no volume info")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenMoreThanOneRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		volumeName := "test-volume"
		transport := &mockTransport{response: &storage.VolumeCreateAccepted{
			Payload: &models.VolumeJobLinkResponse{
				Records: []*models.Volume{{Name: &volumeName}, {Name: &volumeName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.VolumeCreate(&VolumeCreateParams{})
		assert.EqualError(tt, err, "unexpected response from server while creating volume - did not receive exactly one volume")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})
}

func TestAggregateFindByName(t *testing.T) {
	t.Run("WhenAggregateNameIsMissing_ThenReturnError", func(tt *testing.T) {
		storageAPI := storage.New(&mockTransport{}, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.AggregateFindByName(&AggregateCollectionGetParams{})
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Aggregate name missing")
	})

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.AggregateFindByName(&AggregateCollectionGetParams{Name: nillable.ToPointer("aggregate1")})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenMultipleAggregatesReturned_ThenReturnError", func(tt *testing.T) {
		aggregateName := "aggregate1"
		transport := &mockTransport{response: &storage.AggregateCollectionGetOK{
			Payload: &models.AggregateResponse{
				AggregateResponseInlineRecords: []*models.Aggregate{
					{Name: &aggregateName},
					{Name: &aggregateName},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.AggregateFindByName(&AggregateCollectionGetParams{Name: nillable.ToPointer(aggregateName)})
		assert.EqualError(tt, err, "More than one Aggregates returned with the name")
	})

	t.Run("WhenNoAggregatesReturned_ThenReturnNotFoundError", func(tt *testing.T) {
		aggregateName := "aggregate1"
		transport := &mockTransport{response: &storage.AggregateCollectionGetOK{
			Payload: &models.AggregateResponse{
				AggregateResponseInlineRecords: []*models.Aggregate{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.AggregateFindByName(&AggregateCollectionGetParams{Name: nillable.ToPointer(aggregateName)})
		assert.EqualError(tt, err, "aggregate 'aggregate1' not found")
	})

	t.Run("WhenSingleAggregateReturned_ThenReturnAggregate", func(tt *testing.T) {
		aggregateName := "aggregate1"
		transport := &mockTransport{response: &storage.AggregateCollectionGetOK{
			Payload: &models.AggregateResponse{
				AggregateResponseInlineRecords: []*models.Aggregate{
					{Name: &aggregateName},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		aggregate, err := client.AggregateFindByName(&AggregateCollectionGetParams{Name: nillable.ToPointer(aggregateName)})
		assert.NoError(tt, err)
		assert.NotNil(tt, aggregate)
		assert.Equal(tt, aggregateName, *aggregate.Name)
	})
}

func TestAggregateCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.AggregateCollectionGet(&AggregateCollectionGetParams{}, func([]*Aggregate) error { return nil })
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsEmpty_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.AggregateCollectionGetOK{
			Payload: &models.AggregateResponse{
				AggregateResponseInlineRecords: []*models.Aggregate{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.AggregateCollectionGet(&AggregateCollectionGetParams{}, func([]*Aggregate) error { return nil })
		assert.NoError(tt, err)
	})

	t.Run("WhenResponseHasAggregates_ThenReturnAggregates", func(tt *testing.T) {
		aggregateName := "aggregate1"
		transport := &mockTransport{response: &storage.AggregateCollectionGetOK{
			Payload: &models.AggregateResponse{
				AggregateResponseInlineRecords: []*models.Aggregate{
					{Name: &aggregateName},
				},
				NumRecords: nillable.ToPointer(int64(1)),
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		var aggregates []*Aggregate
		err := client.AggregateCollectionGet(&AggregateCollectionGetParams{}, func(a []*Aggregate) error {
			aggregates = a
			return nil
		})
		assert.NoError(tt, err)
		assert.Len(tt, aggregates, 1)
		assert.Equal(tt, aggregateName, *aggregates[0].Name)
	})
}

func TestAggregateModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, _, err := client.AggregateModify(&AggregateModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenAsyncResponseReturned_ThenReturnJob", func(tt *testing.T) {
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.AggregateModifyAccepted{
			Payload: &models.AggregateSimulate{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, job, err := client.AggregateModify(&AggregateModifyParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenSyncResponseReturned_ThenReturnAggregateSimulate", func(tt *testing.T) {
		aggregateName := "aggregate1"
		transport := &mockTransport{response: &storage.AggregateModifyOK{
			Payload: &models.AggregateSimulate{
				AggregateSimulateInlineRecords: []*models.Aggregate{
					{Name: &aggregateName},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		aggregateSimulate, job, err := client.AggregateModify(&AggregateModifyParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, aggregateSimulate)
		assert.Nil(tt, job)
		assert.Equal(tt, aggregateName, *aggregateSimulate.AggregateSimulateInlineRecords[0].Name)
	})
}

func TestSnapshotCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.SnapshotCollectionGet(&SnapshotCollectionGetParams{}, func([]*Snapshot) error { return nil })
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsEmpty_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.SnapshotCollectionGetOK{
			Payload: &models.SnapshotResponse{
				SnapshotResponseInlineRecords: []*models.Snapshot{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.SnapshotCollectionGet(&SnapshotCollectionGetParams{}, func([]*Snapshot) error { return nil })
		assert.NoError(tt, err)
	})

	t.Run("WhenResponseHasSnapshots_ThenReturnSnapshots", func(tt *testing.T) {
		snapshotName := "snapshot1"
		transport := &mockTransport{response: &storage.SnapshotCollectionGetOK{
			Payload: &models.SnapshotResponse{
				SnapshotResponseInlineRecords: []*models.Snapshot{
					{Name: &snapshotName},
				},
				NumRecords: nillable.ToPointer(int64(1)),
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		var snapshots []*Snapshot
		err := client.SnapshotCollectionGet(&SnapshotCollectionGetParams{}, func(s []*Snapshot) error {
			snapshots = s
			return nil
		})
		assert.NoError(tt, err)
		assert.Len(tt, snapshots, 1)
		assert.Equal(tt, snapshotName, *snapshots[0].Name)
	})
}

func TestSnapshotPolicyGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.SnapshotPolicyGet(&SnapshotPolicyGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsSuccessful_ThenReturnSnapshotPolicy", func(tt *testing.T) {
		policyName := "policy1"
		transport := &mockTransport{response: &storage.SnapshotPolicyGetOK{
			Payload: &models.SnapshotPolicy{
				Name: &policyName,
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		policy, err := client.SnapshotPolicyGet(&SnapshotPolicyGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, policy)
		assert.Equal(tt, policyName, *policy.Name)
	})
}

func TestQosPolicyGroupCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.QosPolicyGroupCollectionGet(&QosPolicyGroupCollectionGetParams{}, func([]*QosPolicy) error { return nil })
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsEmpty_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.QosPolicyCollectionGetOK{
			Payload: &models.QosPolicyResponse{
				QosPolicyResponseInlineRecords: []*models.QosPolicy{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.QosPolicyGroupCollectionGet(&QosPolicyGroupCollectionGetParams{}, func([]*QosPolicy) error { return nil })
		assert.NoError(tt, err)
	})

	t.Run("WhenResponseHasQosPolicies_ThenReturnQosPolicies", func(tt *testing.T) {
		policyName := "policy1"
		transport := &mockTransport{response: &storage.QosPolicyCollectionGetOK{
			Payload: &models.QosPolicyResponse{
				QosPolicyResponseInlineRecords: []*models.QosPolicy{
					{Name: &policyName},
				},
				NumRecords: nillable.ToPointer(int64(1)),
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		var qosPolicies []*QosPolicy
		err := client.QosPolicyGroupCollectionGet(&QosPolicyGroupCollectionGetParams{}, func(q []*QosPolicy) error {
			qosPolicies = q
			return nil
		})
		assert.NoError(tt, err)
		assert.Len(tt, qosPolicies, 1)
		assert.Equal(tt, policyName, *qosPolicies[0].Name)
	})
}

func TestQosPolicyGroupCollectionModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, _, err := client.QosPolicyGroupCollectionModify([]*QosPolicyGroupModifyCollectionParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenAsyncResponseReturned_ThenReturnJob", func(tt *testing.T) {
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.QosPolicyModifyCollectionAccepted{
			Payload: &models.QosPolicyJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, job, err := client.QosPolicyGroupCollectionModify([]*QosPolicyGroupModifyCollectionParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenSyncResponseReturned_ThenReturnQosPolicyModifyCollection", func(tt *testing.T) {
		policyName := "policy1"
		transport := &mockTransport{response: &storage.QosPolicyModifyCollectionOK{
			Payload: &models.QosPolicyJobLinkResponse{
				Records: []*models.QosPolicy{
					{Name: &policyName},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		qosPolicyModifyCollection, job, err := client.QosPolicyGroupCollectionModify([]*QosPolicyGroupModifyCollectionParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, qosPolicyModifyCollection)
		assert.Nil(tt, job)
		assert.Len(tt, qosPolicyModifyCollection.Records, 1)
		assert.Equal(tt, policyName, *qosPolicyModifyCollection.Records[0].Name)
	})
}

func TestCloudStoreCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.CloudStoreCreate(&storage.CloudStoreCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, job)
	})

	t.Run("WhenAsyncResponseReturned_ThenReturnJob", func(tt *testing.T) {
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.CloudStoreCreateAccepted{
			Payload: &models.CloudStoreJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.CloudStoreCreate(&storage.CloudStoreCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenNoResponseReturned_ThenReturnNil", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.CloudStoreCreateAccepted{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		job, err := client.CloudStoreCreate(&storage.CloudStoreCreateParams{})
		assert.EqualError(tt, err, "unexpected response from CloudStoreCreate")
		assert.Nil(tt, job)
	})
}

func TestVolumeGet(t *testing.T) {
	t.Run("WhenUUIDIsProvided_ThenReturnVolume", func(tt *testing.T) {
		volumeName := "test-volume"
		transport := &mockTransport{response: &storage.VolumeGetOK{
			Payload: &models.Volume{Name: &volumeName},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		volume, err := client.VolumeGet(&VolumeGetParams{UUID: "someUUID"})
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, volumeName, *volume.Name)
	})

	t.Run("WhenUUIDAndNameAreEmpty_ThenReturnError", func(tt *testing.T) {
		storageAPI := storage.New(&mockTransport{}, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.VolumeGet(&VolumeGetParams{})
		assert.EqualError(tt, err, "UUID and Name parameters cannot be empty when querying for a volume")
	})

	t.Run("WhenVolumeNotFound_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		_, err := client.VolumeGet(&VolumeGetParams{Name: "nonexistent-volume"})
		assert.EqualError(tt, err, "volume 'nonexistent-volume' not found")
	})

	t.Run("WhenVolumeFound_ThenReturnVolume", func(tt *testing.T) {
		volumeName := "test-volume"
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{
					{Name: &volumeName},
				},
			},
		}}
		originalFetchVolumeDetails := FetchVolumeDetails
		defer func() { FetchVolumeDetails = originalFetchVolumeDetails }()
		// Mock implementation
		FetchVolumeDetails = func(sc *storageClient, volume *Volume) (*Volume, error) {
			return &Volume{Volume: models.Volume{Name: nillable.ToPointer("test-volume")}}, nil
		}

		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		volume, err := client.VolumeGet(&VolumeGetParams{Name: "test-volume"})
		assert.NoError(tt, err)
		assert.NotNil(tt, volume)
		assert.Equal(tt, volumeName, *volume.Name)
	})

	t.Run("WhenVolumeFound_ThenReturnVolume_GetVolume_Error", func(tt *testing.T) {
		volumeName := "test-volume"
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{
					{Name: &volumeName},
				},
			},
		}}
		originalFetchVolumeDetails := FetchVolumeDetails
		defer func() { FetchVolumeDetails = originalFetchVolumeDetails }()
		// Mock implementation
		FetchVolumeDetails = func(sc *storageClient, volume *Volume) (*Volume, error) {
			return nil, errors.New("connection error")
		}

		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		volume, err := client.VolumeGet(&VolumeGetParams{Name: "test-volume"})
		assert.Error(tt, err)
		assert.Nil(tt, volume)
	})
}

func TestFetchVolumeDetails(t *testing.T) {
	t.Run("WhenVolumeDetailsFetchFails_ThenReturnError", func(tt *testing.T) {
		client := &storageClient{api: storage.New(&mockTransport{err: errors.New("fetch error")}, nil)}
		volume := &Volume{
			Volume: models.Volume{Name: nillable.ToPointer("test-volume"), UUID: nillable.GetStringPtr("test-uuid")},
		}
		_, err := FetchVolumeDetails(client, volume)
		assert.EqualError(tt, err, "fetch error")
	})

	t.Run("WhenVolumeDetailsFetchSucceeds_ThenReturnVolume", func(tt *testing.T) {
		client := &storageClient{api: storage.New(&mockTransport{
			response: &storage.VolumeGetOK{
				Payload: &models.Volume{Name: nillable.ToPointer("test-volume"), UUID: nillable.GetStringPtr("test-uuid")},
			},
		}, nil)}
		volume := &Volume{Volume: models.Volume{Name: nillable.ToPointer("test-volume"), UUID: nillable.GetStringPtr("test-uuid")}}
		result, err := FetchVolumeDetails(client, volume)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "test-volume", *result.Name)
	})

	t.Run("WhenVolumeDetailsFetchReturnsNil_ThenReturnError", func(tt *testing.T) {
		client := &storageClient{api: storage.New(&mockTransport{
			response: &storage.VolumeGetOK{
				Payload: nil, // Simulating no payload
			},
		}, nil)}
		volume := &Volume{Volume: models.Volume{Name: nillable.ToPointer("test-volume"), UUID: nillable.GetStringPtr("test-uuid")}}
		result, err := FetchVolumeDetails(client, volume)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, "unexpected response from VolumeGet")
	})
}

func TestVolumeCollectionGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeCollectionGet(&VolumeCollectionGetParams{}, func([]*Volume) error { return nil })
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenResponseIsEmpty_ThenReturnNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.VolumeCollectionGet(&VolumeCollectionGetParams{}, func([]*Volume) error { return nil })
		assert.NoError(tt, err)
	})

	t.Run("WhenResponseHasVolumes_ThenReturnVolumes", func(tt *testing.T) {
		volumeName := "volume1"
		transport := &mockTransport{response: &storage.VolumeCollectionGetOK{
			Payload: &models.VolumeResponse{
				VolumeResponseInlineRecords: []*models.Volume{
					{Name: &volumeName},
				},
				NumRecords: nillable.ToPointer(int64(1)),
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		var volumes []*Volume
		err := client.VolumeCollectionGet(&VolumeCollectionGetParams{}, func(v []*Volume) error {
			volumes = v
			return nil
		})
		assert.NoError(tt, err)
		assert.Len(tt, volumes, 1)
		assert.Equal(tt, volumeName, *volumes[0].Name)
	})
}

func TestVolumeModify(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		success, job, err := client.VolumeModify(&VolumeModifyParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.False(tt, success)
		assert.Nil(tt, job)
	})

	t.Run("WhenSyncResponseReturned_ThenReturnSuccess", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.VolumeModifyOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		success, job, err := client.VolumeModify(&VolumeModifyParams{})
		assert.NoError(tt, err)
		assert.True(tt, success)
		assert.Nil(tt, job)
	})

	t.Run("WhenAsyncResponseReturned_ThenReturnJob", func(tt *testing.T) {
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.VolumeModifyAccepted{
			Payload: &models.VolumeJobLinkResponse{
				Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		success, job, err := client.VolumeModify(&VolumeModifyParams{})
		assert.NoError(tt, err)
		assert.False(tt, success)
		assert.NotNil(tt, job)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})
}

func TestSnapshotCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasNoSnapshotInfo_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.SnapshotCreateCreated{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.EqualError(tt, err, "SnapshotCreate invalid created response from storage server - Expected a single record but got: '0'")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenResponseHasMultipleSnapshots_ThenReturnUnexpectedResponseError", func(tt *testing.T) {
		snapshotName1 := "snapshot1"
		snapshotName2 := "snapshot2"
		transport := &mockTransport{response: &storage.SnapshotCreateCreated{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{
					{Name: &snapshotName1},
					{Name: &snapshotName2},
				},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.EqualError(tt, err, "SnapshotCreate invalid created response from storage server - Expected a single record but got: '2'")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenSuccessfulWithCreatedResponse_ThenReturnSnapshot", func(tt *testing.T) {
		snapshotName := "test-snapshot"
		transport := &mockTransport{response: &storage.SnapshotCreateCreated{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{{Name: &snapshotName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Nil(tt, job)
		assert.Equal(tt, snapshotName, *response.Name)
	})

	t.Run("WhenSuccessfulWithAcceptedResponse_ThenReturnSnapshotAndJob", func(tt *testing.T) {
		snapshotName := "test-snapshot"
		UUID := "uuid"
		jobUUID := "job-uuid"
		transport := &mockTransport{response: &storage.SnapshotCreateAccepted{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{{Name: &snapshotName, UUID: &UUID}},
				Job:     &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID(jobUUID))},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.NotNil(tt, job)
		assert.Equal(tt, snapshotName, *response.Name)
		assert.Equal(tt, jobUUID, job.JobUUID)
	})

	t.Run("WhenEmptyRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.SnapshotCreateAccepted{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.ErrorContains(tt, err, "SnapshotCreate invalid accepted response from storage server - Expected a single record but got: '0'")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})

	t.Run("WhenMoreThanOneRecordsInResponse_ThenThrowError", func(tt *testing.T) {
		snapshotName := "test-snapshot"
		transport := &mockTransport{response: &storage.SnapshotCreateAccepted{
			Payload: &models.SnapshotJobLinkResponse{
				Records: []*models.Snapshot{{Name: &snapshotName}, {Name: &snapshotName}},
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		response, job, err := client.SnapshotCreate(&SnapshotCreateParams{})
		assert.EqualError(tt, err, "SnapshotCreate invalid accepted response from storage server - Expected a single record but got: '2'")
		assert.Nil(tt, response)
		assert.Nil(tt, job)
	})
}

func TestSnapshotGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		snapshot, err := client.SnapshotGet(&SnapshotGetParams{})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, snapshot)
	})

	t.Run("WhenResponseIsSuccessful_ThenReturnSnapshot", func(tt *testing.T) {
		snapshotName := "snapshot1"
		transport := &mockTransport{response: &storage.SnapshotGetOK{
			Payload: &models.Snapshot{
				Name: &snapshotName,
			},
		}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		snapshot, err := client.SnapshotGet(&SnapshotGetParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapshot)
		assert.Equal(tt, snapshotName, *snapshot.Name)
	})
}

func TestSnapshotGetParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsIsNil_ThenReturnNil", func(tt *testing.T) {
		result := snapshotGetParamsToONTAP(nil)
		assert.Nil(tt, result)
	})

	t.Run("WhenParamsIsNotNil_ThenFieldsAreMapped", func(tt *testing.T) {
		uuid := "snap-uuid"
		volumeUUID := "vol-uuid"
		params := &SnapshotGetParams{
			UUID:       uuid,
			VolumeUUID: volumeUUID,
		}
		result := snapshotGetParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.Equal(tt, uuid, result.UUID)
		assert.Equal(tt, volumeUUID, result.VolumeUUID)
	})
}

func TestSnapshotDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.SnapshotDelete(&SnapshotDeleteParams{UUID: "someUUID"})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenUuidIsEmpty_ThenThrowError", func(tt *testing.T) {
		transport := &mockTransport{}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.SnapshotDelete(&SnapshotDeleteParams{})
		assert.Error(tt, err)
		assert.EqualError(tt, err, "no UUID provided for SnapshotDelete")
	})

	t.Run("WhenSnapshotUUIDIsPassed_ThenSuccessfullyDeleteSnapshot", func(tt *testing.T) {
		transport := &mockTransport{response: &storage.SnapshotDeleteOK{}}
		storageAPI := storage.New(transport, nil)
		client := &storageClient{api: storageAPI}
		err := client.SnapshotDelete(&SnapshotDeleteParams{UUID: "someUUID"})
		assert.NoError(tt, err)
	})
}

func TestDeleteParamsToONTAP(t *testing.T) {
	t.Run("WhenParamsIsNotNil_ThenFieldsAreMapped", func(tt *testing.T) {
		uuid := "snap-uuid"
		volumeUUID := "vol-uuid"
		params := &SnapshotDeleteParams{
			UUID:       uuid,
			VolumeUUID: volumeUUID,
		}
		result := snapshotDeleteParamsToONTAP(params)
		assert.NotNil(tt, result)
		assert.Equal(tt, uuid, result.UUID)
		assert.Equal(tt, volumeUUID, result.VolumeUUID)
	})
}
