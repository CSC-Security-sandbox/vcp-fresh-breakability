package kms_workflows

import (
	"testing"

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
}
