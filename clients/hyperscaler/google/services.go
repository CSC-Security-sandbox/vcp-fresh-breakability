package google

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/compute/v1"
)

var (
	operationStatus         = _operationStatus
	waitForComputeOperation = _waitForComputeOperation

	timeSleep = time.Sleep
)

func _waitForComputeOperation(gService GcpServices, project, region, operation string) error {
	retryCount := 1
	t2 := time.Now().Add(waitTimeoutMinutes)
	for time.Now().Before(t2) {
		gService.Logger.Debug(fmt.Sprintf("project: %s operation: %s retry attempt : %d", project, operation, retryCount))
		retryCount += 1
		ops, err := gService.AdminGCPService.computeService.RegionOperations.List(project, region).Filter(operation).Do()
		if err != nil {
			return err
		}

		if len(ops.Items) == 0 {
			return errors.New("Unexpected API response. Cannot wait for nothing")
		}

		err = operationStatus(ops.Items)
		if err != nil {
			if !errors.IsNotReadyErr(err) {
				return err
			}

			timeSleep(defaultSleepTime)
			continue
		}

		return nil
	}

	return errors.New("Timeout while confirming google components")
}

func _operationStatus(operations []*compute.Operation) error {
	for _, op := range operations {
		if op.Error != nil {
			var itemErrors []string
			for _, e := range op.Error.Errors {
				if strings.Contains(e.Message, "conflicts with existing") {
					return errors.NewConflictErr(e.Message)
				}

				itemErrors = append(itemErrors, fmt.Sprintf("%s-%s", e.Code, e.Message))
			}

			return fmt.Errorf("errors while waiting for google components: '%s'", strings.Join(itemErrors, ","))
		}

		if op.Status != "DONE" {
			return errors.NewNotReadyErr(op.Name + " not ready")
		}
	}

	return nil
}
