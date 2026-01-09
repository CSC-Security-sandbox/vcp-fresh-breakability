package vsa

import (
	stderrors "errors"

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
		IsShared:      params.IsShared,
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
		// Handle nil CapacityShared - ONTAP defaults to false when omitted
		isShared := false
		if qosPolicy.Fixed != nil && qosPolicy.Fixed.CapacityShared != nil {
			isShared = *qosPolicy.Fixed.CapacityShared
		}
		resp := &QoSGroupPolicyResponse{
			Name:          nillable.FromPointer(qosPolicy.Name),
			UUID:          nillable.FromPointer(qosPolicy.UUID),
			SvmName:       nillable.FromPointer(qosPolicy.Svm.Name),
			MaxThroughput: nillable.FromPointer(qosPolicy.Fixed.MaxThroughputMbps),
			MaxIOPS:       nillable.FromPointer(qosPolicy.Fixed.MaxThroughputIops),
			IsShared:      isShared,
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

	// Validate input parameters
	if params.UUID == "" && params.Name == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, stderrors.New("either UUID or Name must be provided for FindQoSGroupPolicy"))
	}
	if params.UUID != "" && params.Name != "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, stderrors.New("UUID and Name cannot both be provided for FindQoSGroupPolicy"))
	}

	qosPolicy, err := client.Storage().QoSPolicyGroupFind(&ontapRest.QoSPolicyGroupFindParams{
		UUID:    params.UUID,
		Name:    params.Name,
		SvmName: params.SvmName,
	})
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	if qosPolicy != nil {
		// Handle nil CapacityShared - ONTAP defaults to false when omitted
		isShared := false
		if qosPolicy.Fixed != nil && qosPolicy.Fixed.CapacityShared != nil {
			isShared = *qosPolicy.Fixed.CapacityShared
		}
		resp := &QoSGroupPolicyResponse{
			Name:          nillable.FromPointer(qosPolicy.Name),
			UUID:          nillable.FromPointer(qosPolicy.UUID),
			SvmName:       nillable.FromPointer(qosPolicy.Svm.Name),
			MaxThroughput: nillable.FromPointer(qosPolicy.Fixed.MaxThroughputMbps),
			MaxIOPS:       nillable.FromPointer(qosPolicy.Fixed.MaxThroughputIops),
			IsShared:      isShared,
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

func (rc *OntapRestProvider) DeleteQoSGroupPolicy(params DeleteQoSGroupPolicyParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	if params.UUID == "" && params.Name == "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, stderrors.New("either UUID or Name must be provided for DeleteQoSGroupPolicy"))
	}
	if params.UUID != "" && params.Name != "" {
		return vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, stderrors.New("UUID and Name cannot both be provided for DeleteQoSGroupPolicy"))
	}

	job, err := client.Storage().QosPolicyDeleteCollection(&ontapRest.QosPolicyDeleteCollectionParams{
		UUID:    params.UUID,
		Name:    params.Name,
		SvmName: params.SvmName,
	})
	if err != nil {
		// Check if it's a "not found" error - make delete idempotent
		if errors.IsNotFoundErr(err) {
			return nil // Policy doesn't exist, consider deletion successful
		}
		// Check if policy is in use by volumes using error code or conflict error
		// ONTAP returns conflict errors (409) when a resource is in use
		if errors.IsConflictErr(err) {
			return vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, stderrors.New("QoS policy is in use by one or more volumes"))
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}

	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
		}
	}
	return nil
}
