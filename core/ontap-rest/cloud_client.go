package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cloud"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

type CloudTargetCreateCreated = cloud.CloudTargetCreateCreated
type CloudTargetGetOK = cloud.CloudTargetCollectionGetOK
type CloudTargetModifyOK = cloud.CloudTargetModifyOK

type CloudClient interface { // generate:mock
	CloudTargetCreate(params *CloudTargetCreateParams) (*CloudTarget, *JobAccepted, error)
	CloudTargetGet(name *string) (*CloudTarget, error)
	// CloudTargetCollectionGet(params *CloudTargetCollectionGetParams) (*CloudTargetGetOK, error)
	// CloudTargetModify(params *CloudTargetModifyParams) (*CloudTargetModifyOK, *JobAccepted, error)
}

type cloudClient struct {
	api cloud.ClientService
}

func (c *cloudClient) CloudTargetCreate(params *CloudTargetCreateParams) (*CloudTarget, *JobAccepted, error) {
	syncResponse, asyncResponse, err := c.api.CloudTargetCreate(cloudTargetCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}
	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return nil, job, nil
	}

	return &CloudTarget{CloudTarget: *syncResponse.Payload.Records[0]}, nil, nil
}

func (c *cloudClient) CloudTargetGet(name *string) (*CloudTarget, error) {
	resp, err := c.api.CloudTargetCollectionGet(cloudTargetCollectionGetParamsToONTAP(&CloudTargetCollectionGetParams{
		Name: name,
	}), nil)
	// filter based on name
	if err != nil {
		return nil, err
	}
	for _, record := range resp.Payload.CloudTargetResponseInlineRecords {
		if name != nil && *name != *record.Name {
			continue
		}
		return &CloudTarget{CloudTarget: *record}, nil
	}
	return nil, errors.New("cloud target not found")
}

// Uncomment the following methods if you need to implement them
// Note: These methods are commented out as they are not implemented in the original code.
// This Code might be used by restore

// func (c *cloudClient) CloudTargetCollectionGet(params *CloudTargetCollectionGetParams) ([]*CloudTarget, error) {
//	resp, err := c.api.CloudTargetCollectionGet(cloudTargetCollectionGetParamsToONTAP(params), nil)
//	if err != nil {
//		return nil, err
//	}
//
//	return []*CloudTarget{[]*CloudTarget: resp.Payload.CloudTargetResponseInlineRecords}, nil
// }

// func (c *cloudClient) CloudTargetModify(params *CloudTargetModifyParams) (*CloudTarget, *JobAccepted, error) {
//	resp, accepted, err := c.api.CloudTargetModify(cloudTargetModifyParamsToONTAP(params), nil)
//	if err != nil {
//		return nil, nil, err
//	}
//
//	jobAccepted := &JobAccepted{
//		JobUUID: string(*accepted.Payload.Job.SnapmirrorUUID),
//	}
//
//	return resp, jobAccepted, nil
// }
