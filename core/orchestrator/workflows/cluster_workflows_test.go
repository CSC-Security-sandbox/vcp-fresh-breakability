package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestAcceptClusterPeerWorkflow(t *testing.T) {
	t.Run("TestAcceptClusterPeerWorkflow", func(t *testing.T) {
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
		mockStorage := database.NewMockStorage(t)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		ClusterPeerActivity := activities.ClusterPeerActivity{SE: mockStorage}
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		// Set up test data
		pass := "testpass"
		var params = &common.ClusterPeerParams{
			PeerAddresses:      []string{"10.91.0.0", "10.92.0.0"},
			PeerName:           "testPeer",
			AccountName:        "testAccount",
			GeneratePassphrase: false,
			Passphrase:         &pass,
		}
		pool := &datamodel.Pool{
			Username: "test-user",
			Password: "test-password",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
		env.OnActivity(ClusterPeerActivity.AcceptClusterPeer, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Execute workflow
		env.ExecuteWorkflow(AcceptClusterPeerWorkflow, params, pool)

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Nil(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}
