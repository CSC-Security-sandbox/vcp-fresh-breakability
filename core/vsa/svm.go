package vsa

import (
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func (rc *OntapRestProvider) CreateSVM(params CreateSvmParams) (*Svm, error) {
	// Create the SVM
	svm, job, err := rc.client.SVM().SvmCreate(&ontapRest.SvmCreateParams{
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
		if err = rc.client.Poll(job.JobUUID); err != nil {
			return nil, err
		}
	}

	// Validate the SVM response to avoid nil pointer dereferences
	if svm == nil || svm.Name == nil {
		return nil, fmt.Errorf("invalid SVM response from API")
	}

	// Return the created SVM
	return &Svm{
		Name:         *svm.Name,
		ExternalUUID: *svm.UUID,
	}, nil
}
