package kms_workflows

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	env2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestMigrateKmsConfigWorkflow(t *testing.T) {
	params := &common.MigrateKmsConfigParams{
		Name:          "test-pool",
		AccountName:   "test-account",
		ProjectNumber: "123456789",
	}
	t.Run("WhenUpdateJobReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError("Update Job error", "error", nil))

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.ErrorContains(tt, env.GetWorkflowError(), "Update Job error")
		env.AssertExpectations(t)
	})
	t.Run("WhenMigrateSdeKmsConfigActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, temporal.NewNonRetryableApplicationError("Migrate SDE KMS Config Error", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("error"))

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenPollMigrateSdeKmsConfigActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(temporal.NewNonRetryableApplicationError("Polling error", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenPollMigrateSdeKmsConfigActivityReturnsOperationError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(temporal.NewNonRetryableApplicationError("operation failed:", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetPoolsByAccountNameReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Get Pools by account name failed", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenDescribeSDEKmsConfigurationActivityError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})

		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Describe VSA Kms Config failed", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetKmsConfigActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Describe VSA Kms Config failed", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateAndSyncKmsConfigActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("Record not found", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("CreateAndSyncKmsConfigActivity", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateVSAKmsConfigSAKeyActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("Record not found", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("Create VSA Kms Config error", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateVSAKmsConfigGrantRoleActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("Record not found", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("GrantRoleActivity", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateVSAKmsConfigAccessCryptoKeyAndEncryptDataWithImpersonationActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateCreated, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("Record not found", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AccessCryptoKeyAndEncryptDataWithImpersonationActivity", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError(errors.NewNotFoundErr("AccessCryptoKeyAndEncryptDataWithImpersonationActivity", nil).Error(), "KmsConfigNotFound", nil))
		env.OnActivity("DeleteKmsConfig", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenPoolsAreNotValidForMigration", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})

		pool2 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "pool2"}, State: models.LifeCycleStateError}
		pool3 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool3"}, State: models.LifeCycleStateCreating, KmsConfigID: sql.NullInt64{Int64: 1, Valid: true}}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool2, &pool3)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetNodeReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateREADY}
		pool2 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(2), UUID: "pool2"}, State: models.LifeCycleStateREADY}
		var poolsInAccount []*datamodel.Pool
		poolsInAccount = append(poolsInAccount, &pool1, &pool2)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Get Node failed", "error", nil))
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetSvmForPoolIDFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Get SVM for Pool ID failed", "error", nil))
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetSvmForPoolIDReturnsWithIDZero", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM without ID"}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenUpdatingPoolReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Updating Pool state failed", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateEkmForSvmCreateDnsActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()

		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError("Create DNS failed", "error", nil))
		env.OnActivity("DeleteEkmConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateEkmForSvmConfigureKmsForSvmActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Update Kms Config state failed", "error", nil))
		env.OnActivity("DeleteEkmConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateEkmForSvmCheckVsaKmsConfigReachableActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError("Update Kms Config state failed", "error", nil)).Once()
		env.OnActivity("DeleteEkmConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateEkmForSvmUpdatePoolWithKmsConfigActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Update Pool with Kms Config failed", "error", nil))
		env.OnActivity("DeleteEkmConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGetVolumesByPoolIDReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Get volumes by PoolId failed", "error", nil))
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenVolumesForMigrationAreNotPresent", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var volumesForMigration []*datamodel.Volume
		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(volumesForMigration, nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreatedPoolActivityReturnsError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var volumesForMigration []*datamodel.Volume
		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(volumesForMigration, nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("Updating Pool to Ready state failed", "error", nil))
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenVolumesForMigrationArePresent", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var volumesForMigration []*datamodel.Volume
		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}
		volume := &datamodel.Volume{Name: "volName1", BaseModel: datamodel.BaseModel{ID: int64(1)}}
		volumesForMigration = append(volumesForMigration, volume)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(volumesForMigration, nil)
		env.OnActivity("MigrateVsaPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenFutureReturnsWithError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		vsaKmsConfig := datamodel.KmsConfig{State: models.LifeCycleStateREADY}
		env.SetHeader(mockHeader)
		origAuthType := env2.AuthType
		env2.AuthType = env2.USERNAME_PWD_SEC_MGR
		defer func() {
			env2.AuthType = origAuthType
		}()
		env.RegisterWorkflow(MigrateKmsConfigWorkflow)
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&activities.VolumeCreateActivity{})

		var volumesForMigration []*datamodel.Volume
		var poolsInAccount []*datamodel.Pool
		var dbNodes []*datamodel.Node
		pool1 := datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "pool1"}, State: models.LifeCycleStateInUse,
			DeploymentName: "cluster1",
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
			}}
		poolsInAccount = append(poolsInAccount, &pool1)
		node1 := datamodel.Node{Name: "Node", EndpointAddress: "1.2.3.4", HostDNSName: "host1"}
		dbNodes = append(dbNodes, &node1)
		svm := &datamodel.Svm{Name: "SVM with ID", BaseModel: datamodel.BaseModel{ID: int64(1)}}
		volume := &datamodel.Volume{Name: "volName1", BaseModel: datamodel.BaseModel{ID: int64(1)}}
		volumesForMigration = append(volumesForMigration, volume)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("MigrateSdeKmsConfigActivity", mock.Anything, params).Return(nil, nil)
		env.OnActivity("PollMigrateSdeKmsConfigActivity", mock.Anything, params, mock.Anything).Return(nil)
		env.OnActivity("GetPoolsByAccountName", mock.Anything, mock.Anything).Return(poolsInAccount, nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(&vsaKmsConfig, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return(dbNodes, nil)
		env.OnActivity("GetSvmForPoolID", mock.Anything, mock.Anything).Return(svm, nil)
		env.OnActivity("UpdatingPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetVolumesByPoolID", mock.Anything, mock.Anything).Return(volumesForMigration, nil)
		env.OnActivity("MigrateVsaPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError("Updating Pool to Ready state failed", "error", nil))
		env.OnActivity("FailedPoolActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil).Maybe().Maybe()

		env.ExecuteWorkflow(MigrateKmsConfigWorkflow, params)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestValidateKmsConfigForMigration(t *testing.T) { // Generated using GitHub Copilot
	tests := []struct {
		name          string
		state         string
		expectedError error
	}{
		{
			name:          "Valid state READY",
			state:         models.LifeCycleStateREADY,
			expectedError: nil,
		},
		{
			name:          "Valid state IN_USE",
			state:         models.LifeCycleStateInUse,
			expectedError: nil,
		},
		{
			name:          "Invalid state Created",
			state:         models.LifeCycleStateCreated,
			expectedError: errors.NewBadRequestErr("CMEK Configuration needs to be in either Ready or In_Use state for migration"),
		},
		{
			name:          "Transitioning state UPDATING",
			state:         models.LifeCycleStateUpdating,
			expectedError: errors.NewConflictErr(fmt.Sprintf("CMEK Configuration continues to be in transitioning state after SDE migration: %s", models.LifeCycleStateUpdating)),
		},
		{
			name:          "Transitioning state MIGRATING",
			state:         models.LifeCycleStateMigrating,
			expectedError: errors.NewConflictErr(fmt.Sprintf("CMEK Configuration continues to be in transitioning state after SDE migration: %s", models.LifeCycleStateMigrating)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kmsConfig := &datamodel.KmsConfig{
				State: tt.state,
			}
			err := validateKmsConfigForMigration(kmsConfig.State)

			if tt.expectedError == nil {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.expectedError.Error())
			}
		})
	}
}
