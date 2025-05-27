package ontap_rest

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
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
