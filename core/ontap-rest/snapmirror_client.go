package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
)

// SnapmirrorClient describes a snapmirror client
type SnapmirrorClient interface { // generate:mock
	SnapmirrorRelationshipCreate(params *SnapmirrorRelationshipCreateParams) (*SnapmirrorRelationship, *JobAccepted, error)
	SnapmirrorRelationshipResyncOrInitializeOrResume(snapmirrorUUID string) (*SnapmirrorRelationship, *JobAccepted, error)
	SnapmirrorRelationshipDelete(params *SnapmirrorRelationshipDeleteParams) (bool, *JobAccepted, error)
	SnapmirrorRelationshipRelease(params *SnapmirrorRelationshipReleaseParams) (bool, *JobAccepted, error)
	SnapmirrorRelationshipGet(params *SnapmirrorRelationshipGetParams) (*SnapmirrorRelationship, error)
	SnapmirrorRelationshipList(params *SnapmirrorRelationshipListParams) ([]*SnapmirrorRelationship, error)
	SnapmirrorRelationshipListDestinations(params *SnapmirrorRelationshipListDestinationsParams) ([]*SnapmirrorRelationship, error)
	SnapmirrorRelationshipModify(params *SnapmirrorRelationshipModifyParams) (*SnapmirrorRelationship, *JobAccepted, error)
}

type snapmirrorClient struct {
	api snapmirror.ClientService
}

func (s *snapmirrorClient) SnapmirrorRelationshipCreate(params *SnapmirrorRelationshipCreateParams) (*SnapmirrorRelationship, *JobAccepted, error) {
	syncResponse, asyncResponse, err := s.api.SnapmirrorRelationshipCreate(snapmirrorRelationshipCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}

	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return &SnapmirrorRelationship{SnapmirrorRelationship: *asyncResponse.Payload.Records[0]}, job, nil
	}
	return &SnapmirrorRelationship{SnapmirrorRelationship: *syncResponse.Payload.Records[0]}, nil, nil
}

func (s *snapmirrorClient) SnapmirrorRelationshipResyncOrInitializeOrResume(snapmirrorUUID string) (*SnapmirrorRelationship, *JobAccepted, error) {
	syncResponse, asyncResponse, err := s.api.SnapmirrorRelationshipModify(snapmirrorRelationshipSetStateParamsToONTAP(snapmirrorUUID, models.SnapmirrorRelationshipStateSnapmirrored), nil)
	if err != nil {
		return nil, nil, err
	}

	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return nil, job, nil
	}
	return &SnapmirrorRelationship{SnapmirrorRelationship: *syncResponse.Payload.Records[0]}, nil, nil
}

func (s *snapmirrorClient) SnapmirrorRelationshipDelete(params *SnapmirrorRelationshipDeleteParams) (bool, *JobAccepted, error) {
	done, response, err := s.api.SnapmirrorRelationshipDelete(snapmirrorRelationshipDeleteParamsToONTAP(params), nil)
	if err != nil {
		return false, nil, err
	}

	if done != nil {
		return true, nil, nil
	}

	job := &JobAccepted{
		JobUUID: response.Payload.Job.UUID.String(),
	}
	return false, job, nil
}

func (s *snapmirrorClient) SnapmirrorRelationshipRelease(params *SnapmirrorRelationshipReleaseParams) (bool, *JobAccepted, error) {
	done, response, err := s.api.SnapmirrorRelationshipDelete(snapmirrorRelationshipReleaseParamsToONTAP(params), nil)
	if err != nil {
		return false, nil, err
	}

	if done != nil {
		return true, nil, nil
	}

	job := &JobAccepted{
		JobUUID: response.Payload.Job.UUID.String(),
	}
	return false, job, nil
}

func (s *snapmirrorClient) SnapmirrorRelationshipGet(params *SnapmirrorRelationshipGetParams) (*SnapmirrorRelationship, error) {
	response, err := s.api.SnapmirrorRelationshipGet(snapmirrorRelationshipGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}
	return convertSnapmirrorRelationshipGetFromREST(response), err
}

func (s *snapmirrorClient) SnapmirrorRelationshipList(params *SnapmirrorRelationshipListParams) ([]*SnapmirrorRelationship, error) {
	response, err := s.api.SnapmirrorRelationshipsGet(snapmirrorRelationshipListParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	return convertSnapmirrorRelationshipListFromREST(response), err
}

func (s *snapmirrorClient) SnapmirrorRelationshipListDestinations(params *SnapmirrorRelationshipListDestinationsParams) ([]*SnapmirrorRelationship, error) {
	response, err := s.api.SnapmirrorRelationshipsGet(snapmirrorRelationshipListDestinationsParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	return convertSnapmirrorRelationshipListFromREST(response), nil
}

func (s *snapmirrorClient) SnapmirrorRelationshipModify(params *SnapmirrorRelationshipModifyParams) (*SnapmirrorRelationship, *JobAccepted, error) {
	syncResponse, asyncResponse, err := s.api.SnapmirrorRelationshipModify(snapmirrorRelationshipModifyParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}

	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return nil, job, nil
	}
	return &SnapmirrorRelationship{SnapmirrorRelationship: *syncResponse.Payload.Records[0]}, nil, nil
}
