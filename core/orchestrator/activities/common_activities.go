package activities

import "fmt"

type CommonActivities struct{}

// CommonActivities is a struct that represents the common activities for the orchestrator.
func (j CommonActivities) UpdateJobStatus() error {
	fmt.Println("updating job status")
	return nil
}
