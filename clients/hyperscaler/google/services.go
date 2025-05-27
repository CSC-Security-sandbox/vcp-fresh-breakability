package google

import (
	"fmt"
	"strings"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
)

var (
	statusDone        = "DONE"
	operationProgress = int64(100)

	waitForServiceNetworkOperationStatus = _waitForServiceNetworkOperationStatus
	waitForComputeNetGlobalOpStatus      = _waitForComputeNetGlobalOpStatus
	waitForComputeRegionalOperation      = _waitForComputeRegionalOperation
	operationStatus                      = _operationStatus
	waitForComputeOperation              = _waitForComputeOperation

	getServiceNetOpStatus      = _getServiceNetOpStatus
	getComputeGlobalOpStatus   = _getComputeGlobalOpStatus
	getComputeRegionalOpStatus = _getComputeRegionalOpStatus
	timeSleep                  = time.Sleep
)

// _waitForServiceNetworkOperationStatus waits for a service network operation to complete
func _waitForServiceNetworkOperationStatus(gcpService *GcpServices, operationName string) (*models.ComputeOperation, error) {
	retryCount := 1
	t2 := time.Now().Add(waitTimeoutMinutes)
	for time.Now().Before(t2) {
		gcpService.Logger.Debug(fmt.Sprintf("retry count : %d", retryCount))
		retryCount += 1
		timeSleep(defaultSleepTime)
		op, err := getServiceNetOpStatus(gcpService, operationName)
		if err != nil {
			if !errors.IsNotReadyErr(err) {
				gcpService.Logger.Error(fmt.Sprintf("Operation %s failed with error %s", operationName, err.Error()))
				return op, err
			}
			gcpService.Logger.Error(fmt.Sprintf("Retrying operation %s failed with error %s", operationName, err.Error()))
			continue
		}
		if op.Done {
			if op.ErrorResponse != "" {
				gcpService.Logger.Error(fmt.Sprintf("Operation %s completed successfully with error %s", operationName, op.ErrorResponse))
				return op, errors.New(op.ErrorResponse)
			}
			gcpService.Logger.Debug(fmt.Sprintf("Operation %s completed successfully", operationName))
			return op, nil
		}
	}
	gcpService.Logger.Debug("Timeout while confirming service network google components")
	return nil, errors.New("Timeout while confirming service network google components")
}

// _getServiceNetOpStatus returns the status (and result) of a Google's service networking operation
func _getServiceNetOpStatus(gcpService *GcpServices, operation string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.networkingService.Operations.Get(operation).Do()
	if err != nil || (op != nil && op.Error != nil) {
		if err == nil {
			gcpService.Logger.Debug(fmt.Sprintf("getServiceNetOpStatus's operation failed with error : %s", op.Error.Message))
			err = &googleapi.Error{Message: op.Error.Message}
		}
		if err != nil {
			gcpService.Logger.Errorf(fmt.Sprintf("getServiceNetOpStatus failed with error : %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("getServiceNetOpStatus successful : %s", op.Name))
	return convertServiceNetOpToComputeOp(op), nil
}

// _waitForComputeNetGlobalOpStatus waits for the Google's compute global operation to complete
func _waitForComputeNetGlobalOpStatus(gcpService *GcpServices, tenantProject, operationName string) (*models.ComputeOperation, error) {
	retryCount := 1
	t2 := time.Now().Add(waitTimeoutMinutes)
	for time.Now().Before(t2) {
		retryCount += 1
		timeSleep(defaultSleepTime)
		op, err := getComputeGlobalOpStatus(gcpService, tenantProject, operationName)
		if err != nil {
			if !errors.IsNotReadyErr(err) {
				return op, err
			}
			continue
		}

		if op.Status == statusDone && op.Progress == operationProgress {
			return op, nil
		}
	}

	return nil, errors.New("Timeout while confirming compute network google components")
}

// _getComputeOperationStatus returns the status (and result) of a Google's compute Global operation
func _getComputeGlobalOpStatus(gcpService *GcpServices, tenantProjectNumber, operationName string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.computeService.GlobalOperations.Get(tenantProjectNumber, operationName).Do()
	if err != nil || (op != nil && op.Error != nil) {
		if err == nil {
			gcpService.Logger.Debug(fmt.Sprintf("getComputeGlobalOpStatus's operation failed with error : %s", op.Error.Errors[0].Message))
			err = &googleapi.Error{Message: op.Error.Errors[0].Message}
		}
		if err != nil {
			gcpService.Logger.Debug(fmt.Sprintf("getComputeGlobalOpStatus failed with error : %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("getComputeGlobalOpStatus successful : %s", op.Name))
	return convertComputeOpToComputeOp(op), nil
}

// _getComputeRegionalOpStatus returns the status (and result) of a Google's compute regional operation
func _getComputeRegionalOpStatus(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
	op, err := gcpService.AdminGCPService.computeService.RegionOperations.Get(projectNumber, region, operationName).Do()
	if err != nil || (op != nil && op.Error != nil) {
		if err == nil {
			gcpService.Logger.Debug(fmt.Sprintf("getComputeRegionalOpStatus's operation failed with error : %s", op.Error.Errors[0].Message))
			err = &googleapi.Error{Message: op.Error.Errors[0].Message}
		}
		if err != nil {
			gcpService.Logger.Debug(fmt.Sprintf("getComputeRegionalOpStatus failed with error : %s", err.Error()))
			return nil, err
		}
	}
	gcpService.Logger.Debug(fmt.Sprintf("getComputeRegionalOpStatus successful : %s", op.Name))
	return convertComputeOpToComputeOp(op), nil
}

// _waitForComputeNetRegionalOperation waits for a compute network regional operation to complete
func _waitForComputeRegionalOperation(gcpService *GcpServices, projectNumber, region, operationName string) (*models.ComputeOperation, error) {
	retryCount := 1
	t2 := time.Now().Add(waitTimeoutMinutes)
	for time.Now().Before(t2) {
		retryCount += 1
		timeSleep(defaultSleepTime)
		op, err := getComputeRegionalOpStatus(gcpService, projectNumber, region, operationName)
		if err != nil {
			if !errors.IsNotReadyErr(err) {
				return op, err
			}
			continue
		}

		if op.Status == statusDone && op.Progress == operationProgress {
			return op, nil
		}
	}

	return nil, errors.New("Timeout while confirming compute network google components")
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
