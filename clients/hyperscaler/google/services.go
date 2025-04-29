package google

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	waitForServiceNetworkOperationStatus = _waitForServiceNetworkOperationStatus
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
