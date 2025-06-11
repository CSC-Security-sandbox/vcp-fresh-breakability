package ontap_rest

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/snapmirror"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	snapmirrorpriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/snapmirror"
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

	// Priv Client
	SnapmirrorGetPriv(ctx context.Context, destinationPath, relationshipID string, relationshipGroupType *string) (*snapmirrorpriv.SnapmirrorGetOK, error)
}

var (
	snapmirrorGetReturnTimeout = int64(120)
)

type snapmirrorClient struct {
	api     snapmirror.ClientService
	apiPriv snapmirrorpriv.ClientService
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

// SnapmirrorGetPriv retrieves the snapmirror relationship details for a given destination path and relationship ID.
func (s *snapmirrorClient) SnapmirrorGetPriv(ctx context.Context, destinationPath, relationshipID string, relationshipGroupType *string) (*snapmirrorpriv.SnapmirrorGetOK, error) {
	if relationshipID == "" && destinationPath == "" {
		return nil, errors.New("either relationshipID or destinationPath must be provided")
	}

	newCtx, cancel := context.WithTimeout(ctx, time.Duration(snapmirrorGetReturnTimeout)*time.Second)
	defer cancel()

	params := snapmirrorpriv.
		NewSnapmirrorGetParamsWithContext(newCtx).
		WithRelationshipID(&relationshipID).
		WithDestinationPath(&destinationPath).
		WithFields([]string{
			"current-transfer-error",
			"current_operation_id",
			"current_transfer_type",
			"destination_path",
			"destination_endpoint_uuid",
			"healthy",
			"lag_time",
			"last_transfer_end_timestamp",
			"last_transfer_error",
			"last_transfer_size",
			"last_transfer_type",
			"last_transfer_duration",
			"newest_snapshot",
			"throttle",
			"policy",
			"policy_type",
			"progress_last_updated",
			"relationship_id",
			"relationship_type",
			"schedule",
			"source_path",
			"source_volume",
			"state",
			"status",
			"total_progress",
			"total_transfer_bytes",
			"total_transfer_time_secs",
			"type",
			"unhealthy_reason",
			"transfer_snapshot",
			"exported_snapshot",
		}).
		WithReturnTimeout(&snapmirrorGetReturnTimeout)

	expand := true
	if relationshipGroupType != nil {
		params.WithRelationshipGroupType(relationshipGroupType)
		// Todo: Temporarily using string, use const when flexgroup support is added
		if *relationshipGroupType == "flexgroup" {
			// Todo: Check if diag mode is needed for flexgroup
			params.WithExpand(&expand)
			params.WithFields([]string{"total_transfer_bytes", "total_progress", "last_transfer_error"})
		}
	}

	res, err := s.apiPriv.SnapmirrorGet(params)
	if err != nil {
		return nil, err
	}

	return res, nil
}
