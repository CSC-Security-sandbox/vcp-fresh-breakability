package detectors

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// groupByProject groups a flat slice of (projectID, location) pairs into a
// map of projectID → locations and a stable projectOrder slice (first-seen
// insertion order). Both the pool and backup-vault detectors share this
// pattern: enumerate all (project, location) pairs, then fire one CCFE
// workflow per project carrying all its locations.
func groupByProject(pairs []resourcescope.ProjectLocation) (locationsByProject map[string][]string, projectOrder []string) {
	locationsByProject = make(map[string][]string, len(pairs))
	projectOrder = make([]string, 0)
	for _, pair := range pairs {
		if _, seen := locationsByProject[pair.ProjectID]; !seen {
			projectOrder = append(projectOrder, pair.ProjectID)
		}
		locationsByProject[pair.ProjectID] = append(locationsByProject[pair.ProjectID], pair.Location)
	}
	return locationsByProject, projectOrder
}

// logDetectorProgress emits a progress log at every 10 % boundary that has
// not yet been logged. Detectors call this once per project iteration to
// produce consistent "N% done" lines across all resource types.
//
// done is the number of projects completed so far (1-indexed), total is the
// total number of projects, lastDecile is the highest decile already logged
// (updated in-place), detectorName is used as a log prefix.
func logDetectorProgress(logger log.Logger, detectorName string, done, total int, lastDecile *int) {
	decile := (done * 10) / total
	if decile > *lastDecile {
		logger.Infof("%s: progress %d%% (accounts_done=%d accounts_left=%d)",
			detectorName, decile*10, done, total-done)
		*lastDecile = decile
	}
}
