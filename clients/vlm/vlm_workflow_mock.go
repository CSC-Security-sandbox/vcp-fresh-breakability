package vlm

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

type VSAClientWorkflowManagerMock struct {
}

func (vlmManager *VSAClientWorkflowManagerMock) GetClusterZiZsDetails(ctx workflow.Context, req *GetResourceInfoReq) (*GetResourceInfoResp, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock GetClusterZiZsDetails")

	// Create a mock response with sample compliance data
	mockResponse := &GetResourceInfoResp{
		ProjectID:    req.ProjectID,
		DeploymentID: req.DeploymentID,
		ResourceInfo: ResourceInformation{
			GCPRI: map[string][]GCPResourceInformation{
				"pool": {
					{
						SatisfiesPzi: true,  // Mock ZI compliance
						SatisfiesPzs: false, // Mock ZS compliance
						AssetType:    "pool",
						AssetLink:    "mock-asset-link",
					},
				},
			},
		},
	}

	return mockResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) CreateVSAClusterDeployment(ctx workflow.Context, createVSAClusterDeploymentRequest *CreateVSAClusterDeploymentRequest) (*CreateVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock CreateVSAClusterDeployment")
	createVSAClusterDeploymentResponse := &CreateVSAClusterDeploymentResponse{
		VLMConfig: createVSAClusterDeploymentRequest.VLMConfig,
	}

	return createVSAClusterDeploymentResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) CreateVSASVM(ctx workflow.Context, createSVMRequest *CreateSVMRequest) (*CreateSVMResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock CreateVSASVM")
	createSVMResponse := &CreateSVMResponse{
		VLMConfig: createSVMRequest.VLMConfig,
	}

	return createSVMResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) DeleteVSAClusterDeployment(ctx workflow.Context, deleteVSAClusterDeploymentRequest *DeleteVSAClusterDeploymentRequest, ontapVersion string) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock DeleteVSAClusterDeployment")
	return nil
}

func (vlmManager *VSAClientWorkflowManagerMock) UpdateVSAClusterDeployment(ctx workflow.Context, updateVSAClusterDeploymentRequest *UpdateVSAClusterDeploymentRequest, ontapVersion string) (*UpdateVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock UpdateVSAClusterDeployment")
	updateVSAClusterDeploymentResponse := &UpdateVSAClusterDeploymentResponse{
		VLMConfig:    updateVSAClusterDeploymentRequest.VLMConfig,
		UpdateStatus: DeploymentUpdateStatus{},
	}

	return updateVSAClusterDeploymentResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) ValidateClusterHealth(ctx workflow.Context, validateClusterHealthRequest *ValidateClusterHealthRequest) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock ValidateClusterHealth")

	return nil
}

func (vlmManager *VSAClientWorkflowManagerMock) ClusterPowerOp(ctx workflow.Context, clusterPowerOpRequest *ClusterPowerOpRequest) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock ClusterPowerOp", "operation", clusterPowerOpRequest.Operation)

	return nil
}
