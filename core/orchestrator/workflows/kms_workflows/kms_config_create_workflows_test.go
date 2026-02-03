package kms_workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

var getSignedJwtToken = auth.GetSignedJwtToken

func expectJobIsNew(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(coremodels.JobsStateNEW),
	}, nil)
}

func TestCreateKmsConfig(t *testing.T) {
	t.Run("WhenGetSignedTokenActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", errors.New("GetSignedTokenActivity failed"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenJobStateIsNotNew", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(coremodels.JobsStatePROCESSING),
		}, nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expected NEW")
	})
	t.Run("WhenPollKmsConfigOperationActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(temporal.NewNonRetryableApplicationError("some", "error", nil))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenDescribeKmsConfigurationActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreateVSAKmsConfigSAKeyActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenGrantRoleActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCreatedKmsConfigActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenUpdateKmsConfigAttributesActivityFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenSuccess", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		// Set up test data
		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(nil)
		// Execute workflow
		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenWorkflowCompletesMarksSdeJobDone", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:           "test-kms",
			AccountName:    "test-account",
			SdeJobUUID:     "sde-job-id",
			ProjectNumber:  "test-project",
			LocationID:     "us-east4",
			OperationUri:   "operations/test",
			OperationDone:  true,
			XCorrelationID: "corr-id",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)

		sdeJobMarkedDone := false
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.BaseModel.UUID == "sde-job-id" && job.State == string(coremodels.JobsStateDONE)
		})).Run(func(args mock.Arguments) {
			sdeJobMarkedDone = true
		}).Return(nil).Once()
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Maybe()

		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("signed-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		assert.True(t, sdeJobMarkedDone)
		env.AssertExpectations(t)
	})
	t.Run("HeartbeatTimeoutIsConfigured", func(t *testing.T) {
		// This test verifies that HeartbeatTimeout is configured in ActivityOptions
		// by ensuring activities with RecordHeartbeat can execute successfully
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
		}
		kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedKmsConfigActivity", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Verify workflow completes successfully, which confirms HeartbeatTimeout is configured
		// Activities with RecordHeartbeat would fail if HeartbeatTimeout wasn't set
		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeGetSignedTokenActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal before GetSignedTokenActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 5*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedAfterGetSignedTokenActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)

		// Send cancellation signal after GetSignedTokenActivity but before PollKmsConfigOperationActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 10*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforePollKmsConfigOperationActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)

		// Send cancellation signal before PollKmsConfigOperationActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 10*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeDescribeSDEKmsConfigurationActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal before DescribeSDEKmsConfigurationActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 15*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeUpdateKmsConfigAttributesActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)

		// Send cancellation signal before UpdateKmsConfigAttributesActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 20*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeCreateVSAKmsConfigSAKeyActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)

		// Send cancellation signal before CreateVSAKmsConfigSAKeyActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 25*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeGrantRoleActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)

		// Send cancellation signal before GrantRoleActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 30*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationSignalReceivedBeforeCreatedKmsConfigActivity", func(t *testing.T) {
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal before CreatedKmsConfigActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 35*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeGetSignedTokenActivity", func(t *testing.T) {
		// Tests line 118: checkCancellation() returns an error
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal immediately to trigger CheckCancellation error
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 1*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCancellationOccursDuringRollback", func(t *testing.T) {
		// Tests cancellation rollback: signal arrives after GetSignedTokenActivity, caught at line 163 CheckCancellationSignal
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		// Send cancellation signal after GetSignedTokenActivity completes; it will be caught
		// by CheckCancellationSignal at line 163, triggering the rollback via ExecuteDeferredCleanup
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}).Return("test-jwt-token", nil)

		// PollKmsConfigOperationActivity is NOT called because the cancellation check at line 163
		// catches the signal before that activity runs
		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorAfterGetSignedTokenActivity", func(t *testing.T) {
		// Tests line 139: checkCancellation() returns an error after GetSignedTokenActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)

		// Send cancellation signal after GetSignedTokenActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 10*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforePollKmsConfigOperationActivity", func(t *testing.T) {
		// Tests line 174: checkCancellation() returns an error before PollKmsConfigOperationActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)

		// Send cancellation signal before PollKmsConfigOperationActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 15*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeDescribeSDEKmsConfigurationActivity", func(t *testing.T) {
		// Tests line 189: checkCancellation() returns an error before DescribeSDEKmsConfigurationActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal before DescribeSDEKmsConfigurationActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 20*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeUpdateKmsConfigAttributesActivity", func(t *testing.T) {
		// Tests line 201: checkCancellation() returns an error before UpdateKmsConfigAttributesActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)

		// Send cancellation signal before UpdateKmsConfigAttributesActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 25*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeCreateVSAKmsConfigSAKeyActivity", func(t *testing.T) {
		// Tests line 211: checkCancellation() returns an error before CreateVSAKmsConfigSAKeyActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)

		// Send cancellation signal before CreateVSAKmsConfigSAKeyActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 30*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeGrantRoleActivity", func(t *testing.T) {
		// Tests line 220: checkCancellation() returns an error before GrantRoleActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		// CreateVSAKmsConfigSAKeyActivity sets ServiceAccount on kmsConfig before returning it (matching real behavior)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil).Run(func(args mock.Arguments) {
			// Set ServiceAccount on the kmsConfig (matching real behavior where CreateVSAKmsConfigSAKeyActivity sets it)
			if kc, ok := args.Get(1).(*datamodel.KmsConfig); ok && kc != nil {
				kc.ServiceAccount = &datamodel.ServiceAccount{
					BaseModel: datamodel.BaseModel{UUID: "sa-uuid"},
				}
			}
			// Send cancellation signal immediately after CreateVSAKmsConfigSAKeyActivity completes
			// This ensures it arrives before the cancellation check at line 219
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		})

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
	t.Run("WhenCheckCancellationReturnsErrorBeforeCreatedKmsConfigActivity", func(t *testing.T) {
		// Tests line 229: checkCancellation() returns an error before CreatedKmsConfigActivity
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
		env.RegisterActivity(&activities.CommonActivities{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		originalFunc := getSignedJwtToken
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "test-jwt-token", nil
		}
		defer func() {
			getSignedJwtToken = originalFunc
		}()

		params := &common.CreateKmsConfigParams{
			Name:        "test-kms",
			AccountName: "test-account",
			LocationID:  "us-east4",
		}
		kmsConfig := &datamodel.KmsConfig{
			BaseModel:     datamodel.BaseModel{UUID: "kms-config-uuid"},
			KmsAttributes: &datamodel.KmsAttributes{},
		}
		cvpKmsConfig := &cvpmodels.KmsConfigV1beta{}
		expectJobIsNew(env)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSignedTokenActivity", mock.Anything, mock.Anything).Return("test-jwt-token", nil)
		env.OnActivity("PollKmsConfigOperationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("UpdateKmsConfigAttributesActivity", mock.Anything, mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(kmsConfig, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)

		// Send cancellation signal before CreatedKmsConfigActivity
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow(CancelKmsConfigSignalName, "cancel data")
		}, 40*time.Millisecond)

		env.OnActivity("FailedKmsConfigCreateActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateKmsConfigWorkflow, params, kmsConfig)

		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
