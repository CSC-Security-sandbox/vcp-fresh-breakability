package vsa

import (
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
