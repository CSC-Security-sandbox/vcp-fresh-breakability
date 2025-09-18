package background_kms_workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/testsuite"
)

type RotateKmsKeyTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *RotateKmsKeyTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.RegisterWorkflow(RotateKmsSAKeyWorkflow)
}

func (s *RotateKmsKeyTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestRotateKmsKeyTestSuite(t *testing.T) {
	suite.Run(t, new(RotateKmsKeyTestSuite))
}

func (s *RotateKmsKeyTestSuite) TestRotateKmsSAKeyWorkflow_Success() {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	activity := &backgroundactivities.RotateKmsSAKeyActivity{}
	kmsConfigs := []*datamodel.KmsConfig{
		{
			BaseModel: datamodel.BaseModel{UUID: "kms-1"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "sa1@project.iam.gserviceaccount.com",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "kms-2"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "sa2@project.iam.gserviceaccount.com",
			},
		},
	}

	s.env.RegisterActivity(activity.ListKmsConfigs)
	s.env.RegisterActivity(activity.RotateServiceAccountKey)

	s.env.OnActivity(activity.ListKmsConfigs, mock.Anything).Return(kmsConfigs, nil)
	s.env.OnActivity(activity.RotateServiceAccountKey, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(RotateKmsSAKeyWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *RotateKmsKeyTestSuite) TestRotateKmsSAKeyWorkflow_ListKmsConfigsFails() {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	activity := &backgroundactivities.RotateKmsSAKeyActivity{}

	s.env.RegisterActivity(activity.ListKmsConfigs)
	s.env.RegisterActivity(activity.RotateServiceAccountKey)

	s.env.OnActivity(activity.ListKmsConfigs, mock.Anything).Return(nil, errors.New("db error"))

	s.env.ExecuteWorkflow(RotateKmsSAKeyWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "db error")
}

func (s *RotateKmsKeyTestSuite) TestRotateKmsSAKeyWorkflow_RotateServiceAccountKeyFails() {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	activity := &backgroundactivities.RotateKmsSAKeyActivity{}
	kmsConfigs := []*datamodel.KmsConfig{
		{
			BaseModel: datamodel.BaseModel{UUID: "kms-1"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "sa1@project.iam.gserviceaccount.com",
			},
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "kms-2"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "sa2@project.iam.gserviceaccount.com",
			},
		},
	}

	s.env.RegisterActivity(activity.ListKmsConfigs)
	s.env.RegisterActivity(activity.RotateServiceAccountKey)

	s.env.OnActivity(activity.ListKmsConfigs, mock.Anything).Return(kmsConfigs, nil)
	s.env.OnActivity(activity.RotateServiceAccountKey, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("rotation error"))

	s.env.ExecuteWorkflow(RotateKmsSAKeyWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "key rotation failed for one or more service accounts")
}

func (s *RotateKmsKeyTestSuite) TestRotateKmsSAKeyWorkflow_RotationDisabled() {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = false

	activity := &backgroundactivities.RotateKmsSAKeyActivity{}

	// Register activities (even though they shouldn't be called)
	s.env.RegisterActivity(activity.ListKmsConfigs)
	s.env.RegisterActivity(activity.RotateServiceAccountKey)

	// Execute the workflow
	s.env.ExecuteWorkflow(RotateKmsSAKeyWorkflow)

	// Assert
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())

	// Verify that activities were NOT called since rotation is disabled
	s.env.AssertNotCalled(s.T(), "ListKmsConfigs")
	s.env.AssertNotCalled(s.T(), "RotateServiceAccountKey")
}

func (s *RotateKmsKeyTestSuite) TestRotateKmsSAKeyWorkflow_RotationEnabled() {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	activity := &backgroundactivities.RotateKmsSAKeyActivity{}
	kmsConfigs := []*datamodel.KmsConfig{
		{
			BaseModel: datamodel.BaseModel{UUID: "kms-enabled-1"},
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "sa-enabled@project.iam.gserviceaccount.com",
			},
		},
	}

	s.env.RegisterActivity(activity.ListKmsConfigs)
	s.env.RegisterActivity(activity.RotateServiceAccountKey)

	s.env.OnActivity(activity.ListKmsConfigs, mock.Anything).Return(kmsConfigs, nil)
	s.env.OnActivity(activity.RotateServiceAccountKey, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(RotateKmsSAKeyWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())

	// Verify that activities WERE called since rotation is enabled
	s.env.AssertCalled(s.T(), "ListKmsConfigs", mock.Anything)
	s.env.AssertCalled(s.T(), "RotateServiceAccountKey", mock.Anything, mock.Anything, mock.Anything)
}
