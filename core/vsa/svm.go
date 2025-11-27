package vsa

import (
	"context"
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func (rc *OntapRestProvider) CreateSVM(params CreateSvmParams) (*ProviderResponse, error) {
	// Create the SVM
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	svm, job, err := client.SVM().SvmCreate(&ontapRest.SvmCreateParams{
		Name:    params.Name,
		IPSpace: ipSpaceName,
		Protocols: ontapRest.Protocols{
			EnableIscsi: params.Protocols.EnableIscsi,
		},
	})
	if err != nil {
		return nil, err
	}

	// Poll the job if it exists
	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return nil, err
		}
	}

	// Validate the SVM response to avoid nil pointer dereferences
	if svm == nil || svm.Name == nil {
		return nil, fmt.Errorf("invalid SVM response from API")
	}

	// Return the created SVM
	return &ProviderResponse{
		Name:         *svm.Name,
		ExternalUUID: *svm.UUID,
	}, nil
}

func (rc *OntapRestProvider) ModifySVMWithQoSPolicy(params ModifySVMWithQoSPolicyParams) error {
	// Get the ONTAP client
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	// Modify the SVM to apply the QoS policy group
	done, job, err := client.SVM().SvmModify(&ontapRest.SvmModifyParams{
		SvmUUID:       params.SvmUUID,
		QoSPolicyName: &params.QoSPolicyName,
	})
	if err != nil {
		return err
	}

	// Poll the job if it exists
	if job != nil {
		if err = client.Poll(job.JobUUID); err != nil {
			return err
		}
	}

	// If done is true, the operation completed synchronously
	if done {
		return nil
	}

	return nil
}

func (rc *OntapRestProvider) GetSVM(params GetSvmParams) (*ontapRest.Svm, error) {
	// Create the SVM
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	svm, err := client.SVM().SvmGet(&ontapRest.SvmGetParams{
		SvmName: params.Name,
	})
	if err != nil {
		return nil, err
	}

	return svm, nil
}

// ModifyRquota enables or disables recursive quota on an SVM.
//
// This method calls the ONTAP REST API to modify the NFS service's RquotaEnabled setting,
// which controls the 'rquota' (remote quota) protocol support for the SVM.
func (rc *OntapRestProvider) ModifyRquota(ctx context.Context, svmUUID string, rquota bool) error {
	// Get the ONTAP client
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	// Modify the NFS service to enable/disable rquota
	err = client.NAS().NfsParamsModify(ctx, &ontapRest.NfsModifyParams{
		SvmUUID:       svmUUID,
		RquotaEnabled: &rquota,
	})
	if err != nil {
		return err
	}

	return nil
}
