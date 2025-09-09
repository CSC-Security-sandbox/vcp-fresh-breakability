package ontap_rest

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	snapPriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/snapmirror"
	privModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestSnapmirrorRelationshipDelete(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipDelete(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipDeleteAccepted{Payload: &models.SnapmirrorRelationshipJobLinkResponse{Job: &models.JobLink{UUID: nillable.ToPointer(strfmt.UUID("uuid"))}}}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipDelete(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, "uuid", response.JobUUID)
	})
}

func TestSnapmirrorRelationshipGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipGet(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipGetOK{
			Payload: &models.SnapmirrorRelationship{
				UUID: nillable.ToPointer(strfmt.UUID("uuid")),
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipGet(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, "uuid", response.UUID.String())
	})
}

func TestSnapmirrorRelationshipList(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipList(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipsGetOK{
			Payload: &models.SnapmirrorRelationshipResponse{
				SnapmirrorRelationshipResponseInlineRecords: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("uuid")),
					},
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipList(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, "uuid", response[0].UUID.String())
	})
}

func TestSnapmirrorRelationshipCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipCreate(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipCreateCreated{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		snapmirror, _, err := client.SnapmirrorRelationshipCreate(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, ontapResponse.Payload.Records[0].UUID, snapmirror.UUID)
	})
	t.Run("WhenAsyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipCreateAccepted{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("uuid")),
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		snapmirror, response, err := client.SnapmirrorRelationshipCreate(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, ontapResponse.Payload.Records[0].UUID, snapmirror.UUID)
		assert.Equal(tt, ontapResponse.Payload.Job.UUID.String(), response.JobUUID)
	})
}

func TestSnapmirrorRelationshipResyncOrInitialize(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipResyncOrInitializeOrResume("")
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipModifyOK{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		snapmirror, _, err := client.SnapmirrorRelationshipResyncOrInitializeOrResume("")
		assert.NoError(tt, err)
		assert.Equal(tt, ontapResponse.Payload.Records[0].UUID, snapmirror.UUID)
	})
	t.Run("WhenAsyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipModifyAccepted{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("uuid")),
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipResyncOrInitializeOrResume("")
		assert.NoError(tt, err)
		assert.Equal(tt, ontapResponse.Payload.Job.UUID.String(), response.JobUUID)
	})
}

func TestSnapmirrorRelationshipListDestinations(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipListDestinations(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipsGetOK{
			Payload: &models.SnapmirrorRelationshipResponse{
				SnapmirrorRelationshipResponseInlineRecords: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("uuid")),
					},
					{
						UUID: nillable.ToPointer(strfmt.UUID("uuid-2")),
					},
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipListDestinations(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, "uuid", response[0].UUID.String())
		assert.Equal(tt, "uuid-2", response[1].UUID.String())
	})
}

func TestSnapmirrorRelationshipRelease(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, response, err := client.SnapmirrorRelationshipRelease(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})
	t.Run("WhenSyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipDeleteOK{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		success, asyncResponse, err := client.SnapmirrorRelationshipRelease(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, success, asyncResponse == nil)
	})
	t.Run("WhenAsyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipDeleteAccepted{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("uuid")),
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		success, asyncResponse, err := client.SnapmirrorRelationshipRelease(nil)
		assert.NoError(tt, err)
		assert.Equal(tt, success, asyncResponse == nil)
		assert.Equal(tt, ontapResponse.Payload.Job.UUID.String(), asyncResponse.JobUUID)
	})
}

func TestSnapmirrorRelationshipTransferCreate(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		err := client.SnapmirrorRelationshipTransferCreate(nil)
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipTransferCreateCreated{
			Location: "transfer-location",
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		err := client.SnapmirrorRelationshipTransferCreate(nil)
		assert.NoError(tt, err)
	})

	t.Run("WhenUnexpectedResponse", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipTransferCreateCreated{
			Location: "",
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		err := client.SnapmirrorRelationshipTransferCreate(nil)
		assert.EqualError(tt, err, "unexpected response from server while creating Snapmirror Relationship Transfer - received no transfer info")
	})
}

func TestSnapmirrorRelationshipTransferGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipTransferGet(nil)
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, response)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipTransfersGetOK{
			Payload: &models.SnapmirrorTransferResponse{
				NumRecords: nillable.ToPointer(int64(1)),
				SnapmirrorTransferResponseInlineRecords: []*models.SnapmirrorTransfer{
					{UUID: nillable.ToPointer(strfmt.UUID("transfer-uuid"))},
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipTransferGet(nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, response)
		assert.Equal(tt, "transfer-uuid", response.UUID.String())
	})

	t.Run("WhenNoRecords", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorRelationshipTransfersGetOK{
			Payload: &models.SnapmirrorTransferResponse{
				NumRecords: nillable.ToPointer(int64(0)),
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		response, err := client.SnapmirrorRelationshipTransferGet(nil)
		assert.NoError(tt, err)
		assert.Nil(tt, response)
	})
}

func TestSnapmirrorGetPriv(t *testing.T) {
	t.Run("WhenRelationshipGroupTypeIsFlexgroup", func(tt *testing.T) {
		transport := &mockTransport{response: &snapPriv.SnapmirrorGetOK{Payload: &privModels.SnapmirrorResponse{}}}
		privClient := snapPriv.New(transport, nil)
		client := &snapmirrorClient{apiPriv: privClient}

		relationshipGroupType := "flexgroup"
		ctx := context.Background()

		result, err := client.SnapmirrorGetPriv(ctx, "dest-path", "rel-id", &relationshipGroupType)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenRelationshipGroupTypeIsNotFlexgroup", func(tt *testing.T) {
		transport := &mockTransport{response: &snapPriv.SnapmirrorGetOK{Payload: &privModels.SnapmirrorResponse{}}}
		privClient := snapPriv.New(transport, nil)
		client := &snapmirrorClient{apiPriv: privClient}

		relationshipGroupType := "regular"
		ctx := context.Background()

		result, err := client.SnapmirrorGetPriv(ctx, "dest-path", "rel-id", &relationshipGroupType)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenRelationshipGroupTypeIsNil", func(tt *testing.T) {
		transport := &mockTransport{response: &snapPriv.SnapmirrorGetOK{Payload: &privModels.SnapmirrorResponse{}}}
		privClient := snapPriv.New(transport, nil)
		client := &snapmirrorClient{apiPriv: privClient}

		ctx := context.Background()

		result, err := client.SnapmirrorGetPriv(ctx, "dest-path", "rel-id", nil)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
	})

	t.Run("WhenAPICallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("api call failed")}
		privClient := snapPriv.New(transport, nil)
		client := &snapmirrorClient{apiPriv: privClient}

		ctx := context.Background()

		result, err := client.SnapmirrorGetPriv(ctx, "dest-path", "rel-id", nil)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, "api call failed")
	})
}

func TestSnapmirrorObjectStoreEndpointDelete(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorObjstoreEpDeleteOK{}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreEndpointDelete(&SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			EndpointUUID:    "endpoint-uuid",
		})
		assert.NoError(tt, err)
		assert.Nil(tt, job)
	})
	t.Run("WhenSuccessWithJob", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorObjstoreEpDeleteAccepted{
			Payload: &models.ObjectStoreEndpointInfoJobLinkResponse{
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("job-uuid")),
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreEndpointDelete(&SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			EndpointUUID:    "endpoint-uuid",
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid", job.JobUUID)
	})
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreEndpointDelete(&SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			EndpointUUID:    "endpoint-uuid",
		})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, job)
	})
}

func TestSnapmirrorObjectStoreSnapshotDelete(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorObjstoreEpSnapshotDeleteOK{}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreSnapshotDelete(&SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			SnapshotUUID:    "snapshot-uuid",
		})
		assert.NoError(tt, err)
		assert.Nil(tt, job)
	})
	t.Run("WhenSuccessWithJob", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorObjstoreEpSnapshotDeleteAccepted{
			Payload: &models.SnapmirrorObjectStoreEndpointSnapshotJobLinkResponse{
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("job-uuid")),
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreSnapshotDelete(&SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			SnapshotUUID:    "snapshot-uuid",
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid", job.JobUUID)
	})
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		job, err := client.SnapmirrorObjectStoreSnapshotDelete(&SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: "object-store-uuid",
			SnapshotUUID:    "snapshot-uuid",
		})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, job)
	})
}

func TestSnapmirrorObjectStoreSnapshotGet(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.SnapmirrorObjectStoreEndpointSnapshotGetOK{
			Payload: &models.SnapmirrorObjectStoreEndpointSnapshot{
				UUID: nillable.ToPointer(strfmt.UUID("snapshot-uuid")),
				Name: nillable.ToPointer("snapshot-name"),
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		result, err := client.SnapmirrorObjectStoreSnapshotGet(&SnapmirrorCloudSnapshotGetParams{
			ObjectStoreUUID: "object-store-uuid",
			EndpointUUID:    "endpoint-uuid",
			SnapshotUUID:    "snapshot-uuid",
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "snapshot-uuid", result.UUID.String())
		assert.Equal(tt, "snapshot-name", *result.Name)
	})
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		result, err := client.SnapmirrorObjectStoreSnapshotGet(&SnapmirrorCloudSnapshotGetParams{
			ObjectStoreUUID: "object-store-uuid",
			EndpointUUID:    "endpoint-uuid",
			SnapshotUUID:    "snapshot-uuid",
		})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, result)
	})
}

func TestObjectStoreEndpointInfoGet(t *testing.T) {
	t.Run("WhenSuccess", func(tt *testing.T) {
		transport := &mockTransport{response: &snapmirror.ObjectStoreEndpointInfoGetOK{
			Payload: &models.ObjectStoreEndpointInfo{
				UUID: nillable.ToPointer(strfmt.UUID("endpoint-uuid")),
				Destination: &models.ObjectStoreEndpointInfoInlineDestination{
					LogicalSize: nillable.ToPointer(int64(1024)),
				},
			},
		}}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		result, err := client.ObjectStoreEndpointInfoGet(&ObjectStoreEndpointInfoGetParams{
			ObjectStoreUUID: "object-store-uuid",
			UUID:            "endpoint-uuid",
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "endpoint-uuid", result.UUID.String())
		assert.Equal(tt, int64(1024), *result.Destination.LogicalSize)
	})
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		result, err := client.ObjectStoreEndpointInfoGet(&ObjectStoreEndpointInfoGetParams{
			ObjectStoreUUID: "object-store-uuid",
			UUID:            "endpoint-uuid",
		})
		assert.EqualError(tt, err, transport.err.Error())
		assert.Nil(tt, result)
	})
}

func TestSnapmirrorRelationshipReverse(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		_, _, err := client.SnapmirrorRelationshipReverse(&SnapmirrorRelationshipReverseParams{
			UUID:            "test-uuid",
			SourcePath:      "source-path",
			DestinationPath: "dest-path",
		})
		assert.EqualError(tt, err, transport.err.Error())
	})
	t.Run("WhenSyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipModifyOK{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Records: []*models.SnapmirrorRelationship{
					{
						UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
					},
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		snapmirror, asyncResponse, err := client.SnapmirrorRelationshipReverse(&SnapmirrorRelationshipReverseParams{
			UUID:            "test-uuid",
			SourcePath:      "source-path",
			DestinationPath: "dest-path",
		})
		assert.NoError(tt, err)
		assert.NotNil(tt, snapmirror)
		assert.Equal(tt, ontapResponse.Payload.Records[0].UUID, snapmirror.UUID)
		assert.Nil(tt, asyncResponse)
	})
	t.Run("WhenAsyncResponseSuccessful", func(tt *testing.T) {
		ontapResponse := &snapmirror.SnapmirrorRelationshipModifyAccepted{
			Payload: &models.SnapmirrorRelationshipJobLinkResponse{
				Job: &models.JobLink{
					UUID: nillable.ToPointer(strfmt.UUID("job-uuid")),
				},
			},
		}
		transport := &mockTransport{response: ontapResponse}
		n := snapmirror.New(transport, nil)
		client := &snapmirrorClient{api: n}
		snapmirror, asyncResponse, err := client.SnapmirrorRelationshipReverse(&SnapmirrorRelationshipReverseParams{
			UUID:            "test-uuid",
			SourcePath:      "source-path",
			DestinationPath: "dest-path",
		})
		assert.NoError(tt, err)
		assert.Nil(tt, snapmirror)
		assert.NotNil(tt, asyncResponse)
		assert.Equal(tt, ontapResponse.Payload.Job.UUID.String(), asyncResponse.JobUUID)
	})
}
