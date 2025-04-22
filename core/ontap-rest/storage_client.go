package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/storage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

type StorageClient interface { // generate:mock
	AggregateCollectionGet(params *AggregateCollectionGetParams, ucbf UserCallbackFunc[[]*Aggregate]) error
	AggregateFindByName(params *AggregateCollectionGetParams) (*Aggregate, error)
	AggregateModify(params *AggregateModifyParams) (*AggregateSimulate, *JobAccepted, error)

	SnapshotCollectionGet(params *SnapshotCollectionGetParams, ucbf UserCallbackFunc[[]*Snapshot]) error
	SnapshotPolicyGet(params *SnapshotPolicyGetParams) (*SnapshotPolicy, error)

	QosPolicyGroupCollectionGet(params *QosPolicyGroupCollectionGetParams, ucbf UserCallbackFunc[[]*QosPolicy]) error
	QosPolicyGroupCollectionModify(params []*QosPolicyGroupModifyCollectionParams) (*QosPolicyModifyCollection, *JobAccepted, error)

	CloudStoreCreate(params *storage.CloudStoreCreateParams) (*JobAccepted, error)

	RootVolumeGet(params *VolumeGetParams) (*Volume, error)

	VolumeGet(params *VolumeGetParams) (*Volume, error)
	VolumeCollectionGet(params *VolumeCollectionGetParams, ucbf UserCallbackFunc[[]*Volume]) error
	VolumeModify(params *VolumeModifyParams) (bool, *JobAccepted, error)
	VolumeCreate(params *VolumeCreateParams) (*Volume, *JobAccepted, error)
}

type storageClient struct {
	api storage.ClientService
}

// RootVolumeGet invokes pkg/ontap-rest/client/storage/Client.RootVolumeGet
func (sc *storageClient) RootVolumeGet(params *VolumeGetParams) (*Volume, error) {
	response, err := sc.api.VolumeCollectionGet(storage.NewVolumeCollectionGetParams().WithIsSvmRoot(nillable.ToPointer("true")).WithFields(params.Fields), nil)
	if err != nil {
		return nil, err
	}

	if len(response.GetPayload().VolumeResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("Root volume for SVM", params.SvmName)
	}

	if len(response.GetPayload().VolumeResponseInlineRecords) > 1 {
		return nil, errors.New("unexpected response from server while getting root volume in svm: " + nillable.GetString(params.SvmName, ""))
	}

	return &Volume{Volume: *response.GetPayload().VolumeResponseInlineRecords[0]}, nil
}

// VolumeModify invokes pkg/ontap-rest/client/storage/Client.VolumeModify
func (sc *storageClient) VolumeModify(params *VolumeModifyParams) (bool, *JobAccepted, error) {
	// Success code response ignored, since it does not contain any useful data
	okResponse, acceptedResponse, err := sc.api.VolumeModify(volumeModifyParamsToONTAP(params), nil)
	if err != nil {
		return false, nil, err
	}
	if okResponse != nil {
		return true, nil, nil
	}

	job := &JobAccepted{
		JobUUID: acceptedResponse.Payload.Job.UUID.String(),
	}
	return false, job, nil
}

var paginateAggregateCollectionGet = _paginate[[]*Aggregate]

// AggregateCollectionGet invokes pkg/ontap-rest/client/storage/Client.AggregateCollectionGet
func (sc *storageClient) AggregateCollectionGet(params *AggregateCollectionGetParams, ucbf UserCallbackFunc[[]*Aggregate]) error {
	otParams := aggregateCollectionGetParamsToONTAP(params)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	return paginateAggregateCollectionGet(func(next string) ([]*Aggregate, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := sc.api.AggregateCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Aggregate, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, a := range rsp.Payload.AggregateResponseInlineRecords {
			resp[i] = &Aggregate{Aggregate: *a}
		}
		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// AggregateFindByName invokes pkg/ontap-rest/client/storage/Client.AggregateFindByName
func (sc *storageClient) AggregateFindByName(params *AggregateCollectionGetParams) (*Aggregate, error) {
	if nillable.FromPointerWithFallback(params.Name, "") == "" {
		return nil, errors.New("Aggregate name missing")
	}

	otParams := aggregateCollectionGetParamsToONTAP(params)
	rsp, err := sc.api.AggregateCollectionGet(otParams, nil)
	if err != nil {
		return nil, err
	}

	if len(rsp.Payload.AggregateResponseInlineRecords) > 1 {
		return nil, errors.New("More than one Aggregates returned with the name")
	}

	if len(rsp.Payload.AggregateResponseInlineRecords) == 0 {
		return nil, errors.NewNotFoundErr("aggregate", params.Name)
	}

	return &Aggregate{Aggregate: *rsp.Payload.AggregateResponseInlineRecords[0]}, nil
}

// AggregateModify invokes pkg/ontap-rest/client/storage/Client.AggregateModify
func (sc *storageClient) AggregateModify(params *AggregateModifyParams) (*AggregateSimulate, *JobAccepted, error) {
	syncResponse, asyncResponse, err := sc.api.AggregateModify(aggregateModifyParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}
	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return nil, job, nil
	}
	return &AggregateSimulate{AggregateSimulate: *syncResponse.Payload}, nil, nil
}

var paginateQosPolicyGroupCollectionGet = _paginate[[]*QosPolicy]

// QosPolicyGroupCollectionGet invokes pkg/ontap-rest/client/storage/Client.QosPolicyCollectionGet
func (sc *storageClient) QosPolicyGroupCollectionGet(params *QosPolicyGroupCollectionGetParams, ucbf UserCallbackFunc[[]*QosPolicy]) error {
	otParams := qosPolicyGroupCollectionGetParamsToONTAPCollectionGet(params)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	return paginateQosPolicyGroupCollectionGet(func(next string) ([]*QosPolicy, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := sc.api.QosPolicyCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*QosPolicy, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, qos := range rsp.Payload.QosPolicyResponseInlineRecords {
			resp[i] = &QosPolicy{QosPolicy: *qos}
		}
		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// QosPolicyGroupCollectionModify invokes pkg/ontap-rest/client/storage/Client.QosPolicyModifyCollection
func (sc *storageClient) QosPolicyGroupCollectionModify(params []*QosPolicyGroupModifyCollectionParams) (*QosPolicyModifyCollection, *JobAccepted, error) {
	qosModifyparams := qosPolicyGroupCollectionModifyParamsToONTAP(params)
	syncResponse, asyncResponse, err := sc.api.QosPolicyModifyCollection(qosModifyparams, nil)
	if err != nil {
		return nil, nil, err
	}
	if asyncResponse != nil {
		job := &JobAccepted{
			JobUUID: asyncResponse.Payload.Job.UUID.String(),
		}
		return nil, job, nil
	}
	return &QosPolicyModifyCollection{QosPolicyJobLinkResponse: *syncResponse.Payload}, nil, nil
}

var paginateSnapshotCollectionGet = _paginate[[]*Snapshot]

// SnapshotCollectionGet invokes pkg/ontap-rest/client/storage/Client.SnapshotCollectionGet
func (sc *storageClient) SnapshotCollectionGet(params *SnapshotCollectionGetParams, ucbf UserCallbackFunc[[]*Snapshot]) error {
	otParams := snapshotCollectionGetParamsToONTAP(params)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	return paginateSnapshotCollectionGet(func(next string) ([]*Snapshot, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := sc.api.SnapshotCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Snapshot, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, s := range rsp.Payload.SnapshotResponseInlineRecords {
			resp[i] = &Snapshot{Snapshot: *s}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// SnapshotPolicyGet invokes pkg/ontap-rest/client/storage/Client.SnapshotPolicyGet
func (sc *storageClient) SnapshotPolicyGet(params *SnapshotPolicyGetParams) (*SnapshotPolicy, error) {
	response, err := sc.api.SnapshotPolicyGet(snapshotPolicyGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}
	resp := &SnapshotPolicy{SnapshotPolicy: *response.Payload}
	return resp, nil
}

var paginateVolumeCollectionGet = _paginate[[]*Volume]

// VolumeCollectionGet invokes pkg/ontap-rest/client/storage/Client.VolumeCollectionGet
func (sc *storageClient) VolumeCollectionGet(params *VolumeCollectionGetParams, ucbf UserCallbackFunc[[]*Volume]) error {
	otParams := volumeCollectionGetParamsToONTAP(params)
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))

	return paginateVolumeCollectionGet(func(next string) ([]*Volume, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := sc.api.VolumeCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Volume, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, v := range rsp.Payload.VolumeResponseInlineRecords {
			resp[i] = &Volume{Volume: *v}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// VolumeGet invokes pkg/ontap-rest/client/storage/Client.VolumeGet
func (sc *storageClient) VolumeGet(params *VolumeGetParams) (*Volume, error) {
	if params.UUID != "" {
		response, err := sc.api.VolumeGet(volumeGetParamsToONTAP(params), nil)
		if err != nil {
			return nil, err
		}
		resp := &Volume{Volume: *response.Payload}
		return resp, nil
	}

	if params.Name == "" {
		return nil, errors.New("UUID and Name parameters cannot be empty when querying for a volume")
	}

	var vol *Volume
	otParams := volumeCollectionGetParamsToONTAP(&VolumeCollectionGetParams{BaseParams: params.BaseParams, Name: &params.Name})
	otParams.SetMaxRecords(getConstrainedMaxRecords(params.MaxRecords))
	if err := paginateVolumeCollectionGet(func(next string) ([]*Volume, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := sc.api.VolumeCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Volume, len(rsp.Payload.VolumeResponseInlineRecords))
		for i, v := range rsp.Payload.VolumeResponseInlineRecords {
			resp[i] = &Volume{Volume: *v}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, func(volumes []*Volume) error {
		if vol == nil {
			for _, volume := range volumes {
				vol = volume
				break
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if vol == nil {
		return nil, errors.NewNotFoundErr("volume", &params.Name)
	}

	return vol, nil
}

// CloudStoreCreate invokes pkg/ontap-rest/client/storage/Client.CloudStoreCreate
func (sc *storageClient) CloudStoreCreate(params *storage.CloudStoreCreateParams) (*JobAccepted, error) {
	_, responseAccepted, err := sc.api.CloudStoreCreate(params, nil)
	if err != nil {
		return nil, err
	}
	if responseAccepted != nil {
		return &JobAccepted{
			JobUUID: responseAccepted.Payload.Job.UUID.String(),
		}, nil
	}
	return nil, nil
}

// VolumeCreate invokes pkg/ontap-rest/client/storage/Client.VolumeCreate
func (sc *storageClient) VolumeCreate(params *VolumeCreateParams) (*Volume, *JobAccepted, error) {
	created, accepted, err := sc.api.VolumeCreate(volumeCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, nil, err
	}

	if created != nil {
		if len(created.Payload.Records) == 0 {
			return nil, nil, errors.New("unexpected response from server while creating volume - received no volume info")
		}

		if len(created.Payload.Records) > 1 {
			return nil, nil, errors.New("unexpected response from server while creating volume - did not receive exactly one volume")
		}
		return &Volume{Volume: *created.Payload.Records[0]}, nil, nil
	}

	if len(accepted.Payload.Records) == 0 {
		return nil, nil, errors.New("unexpected response from server while creating volume - received no volume info")
	}

	if len(accepted.Payload.Records) > 1 {
		return nil, nil, errors.New("unexpected response from server while creating volume - did not receive exactly one volume")
	}

	return &Volume{Volume: *accepted.Payload.Records[0]}, &JobAccepted{
		JobUUID: string(*accepted.Payload.Job.UUID),
	}, nil
}
