package ontap_rest

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"

var (
	notExactlyOneError = "unexpected response from server while creating FlexCache volume - did not receive exactly one FlexCache volume"
)

// FlexCacheVolumeCreate invokes pkg/ontap-rest/client/storage/Client.VolumeCreate
func (sc *storageClient) FlexCacheVolumeCreate(params *FlexCacheVolumeCreateParams) (*Flexcache, *JobAccepted, error) {
	created, accepted, err := sc.api.FlexcacheCreate(flexCacheVolumeCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}

	if created != nil {
		if len(created.Payload.Records) != 1 {
			return nil, nil, errors.New(notExactlyOneError)
		}
		return &Flexcache{Flexcache: *created.Payload.Records[0]}, nil, nil
	}

	if len(accepted.Payload.Records) != 1 {
		return nil, nil, errors.New(notExactlyOneError)
	}

	return &Flexcache{Flexcache: *accepted.Payload.Records[0]}, &JobAccepted{
		JobUUID: string(*accepted.Payload.Job.UUID),
	}, nil
}

// FlexCacheVolumeDelete invokes pkg/ontap-rest/client/storage/Client.FlexcacheDelete to delete Volume
func (sc *storageClient) FlexCacheVolumeDelete(params *FlexCacheVolumeDeleteParams) (*JobAccepted, error) {
	if params.UUID != "" {
		deleted, accepted, err := sc.api.FlexcacheDelete(flexCacheVolumeDeleteParamsToONTAP(params), nil)
		if err != nil {
			return nil, err
		}

		if accepted != nil {
			return &JobAccepted{
				JobUUID: string(*accepted.Payload.Job.UUID),
			}, nil
		}

		if deleted != nil {
			return nil, nil
		}
	}

	if params.Name == "" {
		return nil, errors.New("no name filter provided for FlexCacheDeleteCollection")
	}

	_, collectionAccepted, err := sc.api.FlexcacheDeleteCollection(flexCacheVolumeDeleteParamsToONTAPCollectionDelete(params), nil)
	if err != nil {
		return nil, err
	}
	if collectionAccepted != nil {
		return &JobAccepted{
			JobUUID: string(*collectionAccepted.Payload.Job.UUID),
		}, nil
	}

	return nil, errors.New("unexpected response from server while deleting FlexCache volume")
}
