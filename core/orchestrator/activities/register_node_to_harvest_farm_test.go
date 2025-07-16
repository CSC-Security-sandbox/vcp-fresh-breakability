package activities

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"gorm.io/gorm"
)

func TestRegisterNodeToHarvestFarm_Success(t *testing.T) {
	mockSE := new(database.MockStorage)
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", PoolID: 42}, {BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", PoolID: 42}}
	maps := []*datamodel.NodeNodeGroupMap{
		{HarvestConfig: &datamodel.HarvestConfig{}},
		{HarvestConfig: &datamodel.HarvestConfig{}},
	}
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(42)).Return(nodes, nil)
	mockSE.On("AssignTwoNodesToTwoGroups", mock.Anything, datamodel.NodeGroupAssignmentParams{
		Node1:            nodes[0],
		Node2:            nodes[1],
		MaxNodesPerGroup: 10,
		CustomerProject:  "customer-project",
		TenantProject:    "tenant-project",
	}).Return(maps, nil)
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	result, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 42, MaxNodesPerGroup: 10, CustomerProjectID: "customer-project", TenantProjectID: "tenant-project"})
	assert.NoError(t, err)
	assert.Equal(t, maps, result)
}

func TestRegisterNodeToHarvestFarm_NoNodes(t *testing.T) {
	mockSE := new(database.MockStorage)
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return([]*datamodel.Node{}, nil)
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", PoolID: 42}, {BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", PoolID: 42}}

	mockSE.On("AssignTwoNodesToTwoGroups", mock.Anything, datamodel.NodeGroupAssignmentParams{
		Node1:            nodes[0],
		Node2:            nodes[1],
		MaxNodesPerGroup: 5,
		CustomerProject:  "customer-project",
		TenantProject:    "tenant-project",
	}).Return(nil, errors.New("assign error"))
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5, CustomerProjectID: "customer-project", TenantProjectID: "tenant-project"})
	assert.Error(t, err)
}

func TestRegisterNodeToHarvestFarm_DBError(t *testing.T) {
	mockSE := new(database.MockStorage)
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nil, errors.New("db error"))
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5, CustomerProjectID: "customer-project", TenantProjectID: "tenant-project"})
	assert.Error(t, err)
}

func TestRegisterNodeToHarvestFarm_AssignError(t *testing.T) {
	mockSE := new(database.MockStorage)
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", PoolID: 1}, {BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", PoolID: 1}}
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockSE.On("AssignTwoNodesToTwoGroups", mock.Anything, datamodel.NodeGroupAssignmentParams{
		Node1:            nodes[0],
		Node2:            nodes[1],
		MaxNodesPerGroup: 5,
		CustomerProject:  "customer-project",
		TenantProject:    "tenant-project",
	}).Return(nil, errors.New("assign error"))
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5, CustomerProjectID: "customer-project", TenantProjectID: "tenant-project"})
	assert.Error(t, err)
}

func TestUploadHarvestTemplate_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := r.ParseMultipartForm(10 << 20)
		assert.NoError(t, err)
		file, _, err := r.FormFile("file")
		assert.NoError(t, err)
		defer func() {
			cerr := file.Close()
			assert.NoError(t, cerr)
		}()
		content, err := io.ReadAll(file)
		assert.NoError(t, err)
		assert.Contains(t, string(content), "fake-yaml")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}, NodeGroup: &datamodel.NodeGroup{LeaseName: "lease-1"}}},
		UploadURL:    ts.URL,
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc:   func() (string, error) { return "template: {{.Fake}}", nil },
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { return "fake-yaml", nil },
	}
	ctx := context.Background()
	assert.NoError(t, activity.UploadHarvestTemplate(ctx, input))
}

func TestUploadHarvestTemplate_RenderError(t *testing.T) {
	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}}},
		UploadURL:    "http://localhost",
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc:   func() (string, error) { return "template", nil },
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { return "", errors.New("render error") },
	}
	ctx := context.Background()
	assert.Error(t, activity.UploadHarvestTemplate(ctx, input))
}

func TestUploadHarvestTemplate_LoadTemplateError(t *testing.T) {
	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}, NodeGroup: &datamodel.NodeGroup{LeaseName: "lease-1"}}},
		UploadURL:    "http://localhost",
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc: func() (string, error) { return "", errors.New("load error") },
	}
	ctx := context.Background()
	assert.Error(t, activity.UploadHarvestTemplate(ctx, input))
}

func TestUploadHarvestTemplate_HTTPError(t *testing.T) {
	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}, NodeGroup: &datamodel.NodeGroup{LeaseName: "lease-1"}}},
		UploadURL:    "http://localhost:0", // invalid port
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc:   func() (string, error) { return "template", nil },
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { return "fake-yaml", nil },
	}
	ctx := context.Background()
	assert.Error(t, activity.UploadHarvestTemplate(ctx, input))
}

// Below test case will test whether k8's lease is been created
func TestValidateAndCreateKubernetesLease_Success(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	nodeGroupsMap := getNodeGroupMap(false, false)
	// nodeGroups := getNodeGroups(false)

	for _, nodeGroupMap := range nodeGroupsMap {
		mockSE.On("UpdateNodeGroup", ctx, nodeGroupMap.NodeGroup).Return(nodeGroupMap.NodeGroup, nil)
	}

	oldCreateKubernetesLease := createKubernetesLease
	// Mock create lease which is in utils
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	mockSE.AssertExpectations(t)
}

// Below test case will test for leaseClient failure
func TestValidateAndCreateKubernetesLease_Failure(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	nodeGroupsMap := getNodeGroupMap(false, false)

	oldCreateKubernetesLease := createKubernetesLease
	// Mock create lease which is in utils
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return errors.New("lease-client-error")
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lease-client-error")
	mockSE.AssertExpectations(t)
}

// Below test case will test that no k8's lease is getting created as LeaseName is already updated
func TestValidateAndCreateKubernetesLease(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	nodeGroupsMap := getNodeGroupMap(false, true)

	err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	mockSE.AssertExpectations(t)
}

// Below test case will test when GetNodeGroup call to DB fails
func TestValidateAndCreateKubernetesLease_DBError(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	nodeGroupsMap := getNodeGroupMap(false, false)

	mockSE.On("UpdateNodeGroup", ctx, nodeGroupsMap[0].NodeGroup).Return(nil, gorm.ErrRecordNotFound)

	oldCreateKubernetesLease := createKubernetesLease
	// Mock create lease which is in utils
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Equal(t, "record not found", err.Error())
	mockSE.AssertExpectations(t)
}

// TestUploadHarvestTemplate_HTTPNon2xx covers the error path for non-2xx HTTP response in UploadHarvestTemplate
func TestUploadHarvestTemplate_HTTPNon2xx(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte("bad request"))
	}))
	defer ts.Close()

	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}, NodeGroup: &datamodel.NodeGroup{LeaseName: "lease-1"}, NodeID: 1}},
		UploadURL:    ts.URL,
	}
	activity := &UploadHarvestTemplateActivity{
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { return "fake-yaml", nil },
	}
	ctx := context.Background()
	err := activity.UploadHarvestTemplate(ctx, input)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upload failed for node mapping")
}
