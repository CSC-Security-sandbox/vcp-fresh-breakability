package common

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"

const (
	OntapJobStateSuccess = "success"
	OntapJobStateFailure = "failure"
	OntapJobStateRunning = "running"
	OntapJobStateQueued  = "queued"
)

type PrepopulateJobStatus struct {
	State        string
	JobUUID      string
	ErrorMessage string
}

func (p *PrepopulateJobStatus) IsComplete() bool {
	return p.State == OntapJobStateSuccess || p.State == OntapJobStateFailure
}

// MapOntapStateToAPIState converts ONTAP job states to API enum values
func MapOntapStateToAPIState(ontapState string) string {
	switch ontapState {
	case OntapJobStateSuccess:
		return models.FlexCacheConfigV1betaCachePrePopulateStateCOMPLETE
	case OntapJobStateFailure:
		return models.FlexCacheConfigV1betaCachePrePopulateStateERROR
	case OntapJobStateRunning:
		return models.FlexCacheConfigV1betaCachePrePopulateStateINPROGRESS
	case OntapJobStateQueued:
		return models.FlexCacheConfigV1betaCachePrePopulateStateINPROGRESS
	default:
		return models.FlexCacheConfigV1betaCachePrePopulateStateCACHEPREPOPULATESTATEUNSPECIFIED
	}
}
