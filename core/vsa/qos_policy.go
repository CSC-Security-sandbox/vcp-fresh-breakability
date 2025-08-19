package vsa

import (
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (rc *OntapRestProvider) CreateQoSGroupPolicy(params CreateQoSGroupPolicyParams) (*QoSGroupPolicyResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	qosPolicy, job, err := client.Storage().QoSPolicyGroupCreate(&ontapRest.QoSPolicyGroupCreateParams{
		Name:          params.Name,
		SvmName:       params.SvmName,
		MaxThroughput: params.MaxThroughput,
		MaxIOPS:       params.MaxIOPS,
	})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
	}

	if qosPolicy != nil {
		resp := &QoSGroupPolicyResponse{
			Name:          nillable.FromPointer(qosPolicy.Name),
			UUID:          nillable.FromPointer(qosPolicy.UUID),
			SvmName:       nillable.FromPointer(qosPolicy.Svm.Name),
			MaxThroughput: nillable.FromPointer(qosPolicy.Fixed.MaxThroughputMbps),
			MaxIOPS:       nillable.FromPointer(qosPolicy.Fixed.MaxThroughputIops),
		}
		return resp, nil
	}
	return nil, err
}

func (rc *OntapRestProvider) FindQoSGroupPolicy(params FindQoSGroupPolicyParams) (*QoSGroupPolicyResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	qosPolicy, err := client.Storage().QoSPolicyGroupFind(&ontapRest.QoSPolicyGroupFindParams{
		Name:    params.Name,
		SvmName: params.SvmName,
	})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	if qosPolicy != nil {
		resp := &QoSGroupPolicyResponse{
			Name:          nillable.FromPointer(qosPolicy.Name),
			UUID:          nillable.FromPointer(qosPolicy.UUID),
			SvmName:       nillable.FromPointer(qosPolicy.Svm.Name),
			MaxThroughput: nillable.FromPointer(qosPolicy.Fixed.MaxThroughputMbps),
			MaxIOPS:       nillable.FromPointer(qosPolicy.Fixed.MaxThroughputIops),
		}
		return resp, nil
	}
	return nil, err
}

func (rc *OntapRestProvider) UpdateQoSGroupPolicy(params UpdateQoSGroupPolicyParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	job, err := client.Storage().QoSPolicyGroupUpdate(&ontapRest.QoSPolicyGroupUpdateParams{
		UUID:          params.UUID,
		Name:          params.Name,
		SvmName:       params.SvmName,
		MaxThroughput: params.MaxThroughput,
		MaxIOPS:       params.MaxIOPS,
	})
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.NewNotFoundErr("QoS Policy", nil))
	}

	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
	}
	return nil
}
