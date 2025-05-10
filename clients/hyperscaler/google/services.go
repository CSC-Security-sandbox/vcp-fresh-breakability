package google

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	waitForServiceNetworkOperationStatus = _waitForServiceNetworkOperationStatus
	operationStatus                      = _operationStatus
	waitForComputeOperation              = _waitForComputeOperation
)

// _waitForServiceNetworkOperationStatus waits for a service network operation to complete
func _waitForServiceNetworkOperationStatus(gcpService *GcpServices, operationName string) (*servicenetworking.Operation, error) {
	retryCount := 1
	t2 := time.Now().Add(waitTimeoutMinutes)
	for time.Now().Before(t2) {
		gcpService.Logger.Debug(fmt.Sprintf("retry count : %d", retryCount))
		retryCount += 1
		time.Sleep(defaultSleepTime)
		op, err := getNetworkingOperationStatus(gcpService, operationName)
		if err != nil {
			if !errors.IsNotReadyErr(err) {
				gcpService.Logger.Debug(fmt.Sprintf("Operation %s failed with error %s", operationName, err.Error()))
				return op, err
			}
			continue
		}

		if op.Done {
			if op.Error != nil {
				gcpService.Logger.Debug(fmt.Sprintf("Operation %s completed successfully with error %s", operationName, op.Error.Message))
				return op, errors.New(op.Error.Message)
			}
			gcpService.Logger.Debug(fmt.Sprintf("Operation %s completed successfully", operationName))
			return op, nil
		}
	}
	gcpService.Logger.Debug("Timeout while confirming service network google components")
	return nil, errors.New("Timeout while confirming service network google components") // TODO: add retry of creation of
}

// getNetworkingOperationStatus returns the status (and result) of a Google's operation
func getNetworkingOperationStatus(gcpService *GcpServices, operation string) (*servicenetworking.Operation, error) {
	op, err := gcpService.AdminGCPService.networkingService.Operations.Get(operation).Do()
	if err != nil || (op != nil && op.Error != nil) {
		if err == nil {
			gcpService.Logger.Debug(fmt.Sprintf("getNetworkingOperationStatus 's operation failed with error : %s", op.Error.Message))
			err = &googleapi.Error{Message: op.Error.Message}
		}
		if err != nil {
			gcpService.Logger.Debug(fmt.Sprintf("getNetworkingOperationStatus failed with error : %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("getNetworkingOperationStatus successful : %s", op.Name))
	return op, nil
}

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

			time.Sleep(defaultSleepTime)
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
