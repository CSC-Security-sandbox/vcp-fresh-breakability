package vlm

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

type VSAClientWorkflowManagerMock struct {
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
