package vsa

import (
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func (rc *OntapRestProvider) SnapmirrorRelationshipCreate(destinationPath, sourcePath string, smcToken *string) (*ontapRest.SnapmirrorRelationship, error) {
	client := getOntapClientFunc(rc.ClientParams)
	_, job, err := client.Snapmirror().SnapmirrorRelationshipCreate(&ontapRest.SnapmirrorRelationshipCreateParams{SourcePath: sourcePath, DestinationPath: destinationPath, AccessToken: smcToken})
	if err != nil {
		return nil, err
	}
	err = waitForJobIfNeeded(rc, job)
	if err != nil {
		return nil, err
	}

	snapmirror, err := client.Snapmirror().SnapmirrorRelationshipList(&ontapRest.SnapmirrorRelationshipListParams{SourcePath: sourcePath, DestinationPath: destinationPath})
	if err != nil {
		return nil, err
	}
	if len(snapmirror) == 0 {
		return nil, fmt.Errorf("snapmirror relationship not found for destination: %s and source: %s", destinationPath, sourcePath)
	}
	// there can be only one snapmirror relationship for a given source and destination path for backup
	return snapmirror[0], nil
}

func (rc *OntapRestProvider) SnapmirrorRelationshipDelete(UUID string) (*OntapAsyncResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	_, job, err := client.Snapmirror().SnapmirrorRelationshipDelete(&ontapRest.SnapmirrorRelationshipDeleteParams{UUID: UUID})
	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}

func (rc *OntapRestProvider) SnapmirrorRelationshipGet(destinationPath, sourcePath string) (*ontapRest.SnapmirrorRelationship, error) {
	client := getOntapClientFunc(rc.ClientParams)
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
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Snapmirror().SnapmirrorRelationshipTransferCreate(&ontapRest.SnapmirrorRelationshipTransferCreateParams{UUID: snapmirrorUUID, SnapshotName: snapshotName, AccessToken: smcToken})
	if err != nil {
		return err
	}
	return nil
}

func (rc *OntapRestProvider) SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName string) (*ontapRest.SnapmirrorTransfer, error) {
	client := getOntapClientFunc(rc.ClientParams)
	snapmirrorTransfer, err := client.Snapmirror().SnapmirrorRelationshipTransferGet(&ontapRest.SnapmirrorRelationshipTransferGetParams{SnapmirrorUUID: snapmirrorUUID, SnapshotName: snapshotName})
	if err != nil {
		return nil, err
	}
	return snapmirrorTransfer, nil
}

func (rc *OntapRestProvider) SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, EndpointUUID string) (*OntapAsyncResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	job, err := client.Snapmirror().SnapmirrorObjectStoreEndpointDelete(&ontapRest.SnapmirrorCloudEndpointDeleteParams{
		ObjectStoreUUID: objectStoreUUID,
		EndpointUUID:    EndpointUUID,
	})
	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}

func (rc *OntapRestProvider) SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, EndpointUUID, snapshotUUID string) (*OntapAsyncResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	job, err := client.Snapmirror().SnapmirrorObjectStoreSnapshotDelete(&ontapRest.SnapmirrorCloudSnapshotDeleteParams{
		ObjectStoreUUID: objectStoreUUID,
		EndpointUUID:    EndpointUUID,
		SnapshotUUID:    snapshotUUID,
	})
	if job != nil {
		return &OntapAsyncResponse{JobUUID: job.JobUUID}, err
	}
	return nil, err
}
