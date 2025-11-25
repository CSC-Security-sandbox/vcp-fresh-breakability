package ontap_rest

import (
	"context"
	"fmt"
	"time"

	ontapRestModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// PollOntapJobDirectly polls an ONTAP job directly without using workflow
func PollOntapJobDirectly(ctx context.Context, client RESTClient, jobUUID string, maxDuration time.Duration, pollInterval time.Duration, logger log.Logger) error {
	startTime := time.Now()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("job polling cancelled: %w", ctx.Err())
		default:
		}

		if time.Since(startTime) > maxDuration {
			return fmt.Errorf("job polling timeout after %v", maxDuration)
		}

		jobResponse, err := client.Cluster().GetJob(jobUUID)
		if err != nil {
			// Wait for next poll interval or context cancellation
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return fmt.Errorf("job polling cancelled: %w", ctx.Err())
			}
		}

		if jobResponse.Payload.State == nil {
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return fmt.Errorf("job polling cancelled: %w", ctx.Err())
			}
		}

		jobState := *jobResponse.Payload.State
		jobMessage := ""
		if jobResponse.Payload.Message != nil {
			jobMessage = *jobResponse.Payload.Message
		}

		switch jobState {
		case ontapRestModels.JobStateSuccess:
			return nil
		case ontapRestModels.JobStateFailure:
			return fmt.Errorf("job failed: %s", jobMessage)
		case ontapRestModels.JobStateQueued, ontapRestModels.JobStateRunning, ontapRestModels.JobStatePaused:
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return fmt.Errorf("job polling cancelled: %w", ctx.Err())
			}
		default:
			select {
			case <-ticker.C:
				continue
			case <-ctx.Done():
				return fmt.Errorf("job polling cancelled: %w", ctx.Err())
			}
		}
	}
}
