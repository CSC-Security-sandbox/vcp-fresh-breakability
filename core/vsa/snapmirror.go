package vsa

import (
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func (rc *OntapRestProvider) SnapmirrorRelationshipCreate(params *commonparams.SnapmirrorRelationshipParams, smcToken *string) (*ontapRest.SnapmirrorRelationship, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	_, job, err := client.Snapmirror().SnapmirrorRelationshipCreate(&ontapRest.SnapmirrorRelationshipCreateParams{SourcePath: params.SourcePath, DestinationPath: params.DestinationPath, AccessToken: smcToken, IsRestore: params.IsRestore, SourceUUID: params.SourceUUID})
	if err != nil {
		return nil, err
	}
	err = waitForJobIfNeeded(rc, job)
	if err != nil {
		return nil, err
	}

	snapmirror, err := client.Snapmirror().SnapmirrorRelationshipList(&ontapRest.SnapmirrorRelationshipListParams{SourcePath: params.SourcePath, DestinationPath: params.DestinationPath})
	if err != nil {
		return nil, err
	}
	if len(snapmirror) == 0 {
		return nil, fmt.Errorf("snapmirror relationship not found for destination: %s and source: %s", params.DestinationPath, params.SourcePath)
	}
	// there can be only one snapmirror relationship for a given source and destination path for backup
	return snapmirror[0], nil
}

// SnapmirrorRelationshipDelete Enhanced SnapmirrorRelationshipDelete with idempotency
func (rc *OntapRestProvider) SnapmirrorRelationshipDelete(UUID string) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	_, job, err := client.Snapmirror().SnapmirrorRelationshipDelete(&ontapRest.SnapmirrorRelationshipDeleteParams{UUID: UUID})
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// Relationship was deleted by another process, treat as success
			return nil, nil
		}
		return nil, err
	}

	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}

func (rc *OntapRestProvider) SnapmirrorRelationshipGet(destinationPath, sourcePath string) (*ontapRest.SnapmirrorRelationship, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	snapmirror, err := client.Snapmirror().SnapmirrorRelationshipList(&ontapRest.SnapmirrorRelationshipListParams{DestinationPath: destinationPath, SourcePath: sourcePath})
	if err != nil {
		return nil, err
	}
	if len(snapmirror) == 0 {
		return nil, fmt.Errorf("snapmirror relationship not found for destination: %s and source: %s", destinationPath, sourcePath)
	}
	return snapmirror[0], nil
}

func (rc *OntapRestProvider) SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName string, smcToken *string) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	err = client.Snapmirror().SnapmirrorRelationshipTransferCreate(&ontapRest.SnapmirrorRelationshipTransferCreateParams{UUID: snapmirrorUUID, SnapshotName: snapshotName, AccessToken: smcToken})
	if err != nil {
		return err
	}
	return nil
}

func (rc *OntapRestProvider) SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName string) (*ontapRest.SnapmirrorTransfer, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	snapmirrorTransfer, err := client.Snapmirror().SnapmirrorRelationshipTransferGet(&ontapRest.SnapmirrorRelationshipTransferGetParams{SnapmirrorUUID: snapmirrorUUID, SnapshotName: snapshotName})
	if err != nil {
		return nil, err
	}
	return snapmirrorTransfer, nil
}

// SnapmirrorObjectStoreEndpointDelete Enhanced SnapmirrorObjectStoreEndpointDelete with idempotency
func (rc *OntapRestProvider) SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, EndpointUUID string) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	job, err := client.Snapmirror().SnapmirrorObjectStoreEndpointDelete(&ontapRest.SnapmirrorCloudEndpointDeleteParams{
		ObjectStoreUUID: objectStoreUUID,
		EndpointUUID:    EndpointUUID,
	})
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// Endpoint was deleted by another process, treat as success
			return nil, nil
		}
		return nil, err
	}

	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}

// SnapmirrorObjectStoreSnapshotDelete Enhanced SnapmirrorObjectStoreSnapshotDelete with idempotency
func (rc *OntapRestProvider) SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, EndpointUUID, snapshotUUID string) (*OntapAsyncResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	job, err := client.Snapmirror().SnapmirrorObjectStoreSnapshotDelete(&ontapRest.SnapmirrorCloudSnapshotDeleteParams{
		ObjectStoreUUID: objectStoreUUID,
		EndpointUUID:    EndpointUUID,
		SnapshotUUID:    snapshotUUID,
	})
	if err != nil {
		if errors.IsNotFoundErr(err) {
			// Snapshot was deleted by another process, treat as success
			return nil, nil
		}
		return nil, err
	}

	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}

func (rc *OntapRestProvider) SnapmirrorObjectStoreSnapshotGet(objectStoreUUID, EndpointUUID, snapshotUUID string) (*SmObjectStoreEndpointSnapshot, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	resp, err := client.Snapmirror().SnapmirrorObjectStoreSnapshotGet(&ontapRest.SnapmirrorCloudSnapshotGetParams{
		ObjectStoreUUID: objectStoreUUID,
		EndpointUUID:    EndpointUUID,
		SnapshotUUID:    snapshotUUID,
	})
	if err != nil {
		if err.Error() == "snapshot not found" {
			return nil, fmt.Errorf("snapshot %s not found in object store", snapshotUUID)
		}
		return nil, err
	}
	if resp != nil {
		// Commenting this code as we are not checking the snapshot state for now, we can add it later if needed
		// if resp.SnapshotState == nil || *resp.SnapshotState != transferredSnapshotState {
		//	return nil, fmt.Errorf("snapshot %s is not in valid state, current state: %v", snapshotUUID, resp.SnapshotState)
		// }
		return &SmObjectStoreEndpointSnapshot{
			UUID:              resp.UUID,
			Name:              resp.Name,
			ArchivedObjects:   resp.ArchivedObjects,
			GroupMemberCount:  resp.GroupMemberCount,
			LogicalSize:       resp.LogicalSize,
			SnapshotLockState: resp.SnapshotLockState,
			CreateTime:        resp.CreateTime,
			SnapshotState:     resp.SnapshotState,
			SnapmirrorLabel:   resp.SnapmirrorLabel,
		}, nil
	}
	return nil, fmt.Errorf("snapshot %s not found in object store", snapshotUUID)
}

func (rc *OntapRestProvider) ObjectStoreEndpointInfoGet(objectStoreUUID, EndpointUUID string) (*SmObjectStoreEndpointt, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	resp, err := client.Snapmirror().ObjectStoreEndpointInfoGet(&ontapRest.ObjectStoreEndpointInfoGetParams{
		ObjectStoreUUID: objectStoreUUID,
		UUID:            EndpointUUID,
	})
	if err != nil {
		return nil, err
	}
	if resp != nil {
		return &SmObjectStoreEndpointt{
			LogicalSize: resp.Destination.LogicalSize,
			UUID:        resp.UUID,
		}, nil
	}
	return nil, fmt.Errorf("object store endpoint %s not found in object store %s", EndpointUUID, objectStoreUUID)
}
