package vlm

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

type VSAClientWorkflowManagerMock struct {
}

func (vlmManager *VSAClientWorkflowManagerMock) ValidateClusterHealth(ctx workflow.Context, validateClusterHealthRequest *ValidateClusterHealthRequest) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock ValidateClusterHealth")
	return nil
}

func (vlmManager *VSAClientWorkflowManagerMock) ClusterPowerOp(ctx workflow.Context, clusterPowerOpRequest *ClusterPowerOpReq) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock ClusterPowerOp")
	return nil
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

func (vlmManager *VSAClientWorkflowManagerMock) UpgradeVSAClusterDeployment(ctx workflow.Context, upgradeVSAClusterDeploymentRequest *UpdateVSAClusterDeploymentRequest, ontapVersion string) (*UpgradeVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock UpgradeVSAClusterDeployment")
	upgradeVSAClusterDeploymentResponse := &UpgradeVSAClusterDeploymentResponse{
		VLMConfig:    upgradeVSAClusterDeploymentRequest.VLMConfig,
		OntapVersion: upgradeVSAClusterDeploymentRequest.OntapUpgrade.OntapUpgradeTargetImageVersion,
	}

	return upgradeVSAClusterDeploymentResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) UpgradeVSAClusterDeploymentWorkflow(ctx workflow.Context, req *UpdateVSAClusterDeploymentRequest) (*UpgradeVSAClusterDeploymentResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock UpgradeVSAClusterDeploymentWorkflow")
	upgradeVSAClusterDeploymentResponse := &UpgradeVSAClusterDeploymentResponse{
		VLMConfig:    req.VLMConfig,
		OntapVersion: req.OntapUpgrade.OntapUpgradeTargetImageVersion,
	}

	return upgradeVSAClusterDeploymentResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) UpgradeVSAMediatorWorkflow(ctx workflow.Context, req *UpdateMediatorRequest) (*UpdateMediatorResponse, error) {
	logger := util.GetLogger(ctx)
	logger.Info("Mock UpgradeVSAMediatorWorkflow")
	upgradeVSAMediatorResponse := &UpdateMediatorResponse{
		VLMConfig: req.VLMConfig,
	}

	return upgradeVSAMediatorResponse, nil
}

func (vlmManager *VSAClientWorkflowManagerMock) UpdateLicenseWorkflow(ctx workflow.Context, req *UpdateLicenseRequest) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock UpdateLicenseWorkflow", "vsaManagementIP", req.VSAManagementIP)
	return nil
}

func (vlmManager *VSAClientWorkflowManagerMock) CreateVSAExpertModeUser(ctx workflow.Context, createVSAExpertModeUserRequest *OntapExpertModeUserConfig) error {
	logger := util.GetLogger(ctx)
	logger.Info("Mock GetVSAClusterDeployment")
	return nil
}
