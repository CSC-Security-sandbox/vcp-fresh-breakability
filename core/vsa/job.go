package vsa

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
)

func (rc *OntapRestProvider) JobGet(jobUUID string) (*OntapJob, error) {
	client := getOntapClientFunc(rc.ClientParams)
	job, err := client.Cluster().GetJob(jobUUID)
	if err != nil {
		return nil, err
	}
	return convertOntapJobToVSA(job.Payload), nil
}

func convertOntapJobToVSA(job *models.Job) *OntapJob {
	vsaJobModel := &OntapJob{
		UUID: job.UUID.String(),
	}
	if job.State != nil {
		vsaJobModel.State = *job.State
	}
	if job.Error != nil {
		vsaJobModel.Error = &OntapError{}
		if job.Error.Code != nil {
			vsaJobModel.Error.Code = *job.Error.Code
		}
		if job.Error.Message != nil {
			vsaJobModel.Error.Message = *job.Error.Message
		}
	}
	return vsaJobModel
}
