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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
	assert.Contains(t, err.Error(), "not enough nodes found for pool")
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

func TestUploadHarvestTemplate_WithCredentials(t *testing.T) {
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

	harvestConfig := &datamodel.HarvestConfig{}
	input := UploadHarvestTemplateInput{
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: harvestConfig, NodeGroup: &datamodel.NodeGroup{LeaseName: "lease-1"}}},
		UploadURL:    ts.URL,
		Credentials:  &vlm.OntapCredentials{AdminPassword: "test-password"},
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc:   func() (string, error) { return "template: {{.Fake}}", nil },
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { 
			// Verify that the password was set from credentials
			assert.Equal(t, "test-password", cfg.PASSWORD)
			return "fake-yaml", nil 
		},
	}
	ctx := context.Background()
	assert.NoError(t, activity.UploadHarvestTemplate(ctx, input))
	
	// Verify that the password was actually set in the HarvestConfig
	assert.Equal(t, "test-password", harvestConfig.PASSWORD)
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

	// Create test node maps with proper initialization
	nodeGroup1 := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-uuid-1"},
		Name:      "test-group-1",
	}
	nodeGroup2 := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 2, UUID: "test-uuid-2"},
		Name:      "test-group-2",
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup1,
			NodeGroupID:   nodeGroup1.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
		{
			BaseModel:     datamodel.BaseModel{ID: 2},
			NodeID:        2,
			NodeGroup:     nodeGroup2,
			NodeGroupID:   nodeGroup2.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	for _, nodeGroupMap := range nodeGroupsMap {
		mockSE.On("UpdateNodeGroup", ctx, nodeGroupMap.NodeGroup).Return(nodeGroupMap.NodeGroup, nil)
		mockSE.On("UpdateNodeNodeGroupMap", ctx, nodeGroupMap).Return(nodeGroupMap, nil)
	}

	oldCreateKubernetesLease := createKubernetesLease
	// Mock create lease which is in utils
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	assert.NotNil(t, updatedMappings)
	assert.Len(t, updatedMappings, len(nodeGroupsMap))
	for _, mapping := range updatedMappings {
		assert.NotNil(t, mapping.NodeGroup)
		assert.NotEmpty(t, mapping.NodeGroup.LeaseName)
		assert.Equal(t, mapping.NodeGroup.LeaseName, mapping.HarvestConfig.LEASE_NAME)
	}
	mockSE.AssertExpectations(t)
}

// Below test case will test for leaseClient failure
func TestValidateAndCreateKubernetesLease_Failure(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node map with proper initialization
	nodeGroup := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-uuid-1"},
		Name:      "test-group-1",
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup,
			NodeGroupID:   nodeGroup.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock lease creation to fail first
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		// Verify that the lease name matches what we expect
		expectedLeaseName := leasePrefix + nodeGroup.UUID
		if leaseName != expectedLeaseName {
			t.Errorf("Expected lease name %s, got %s", expectedLeaseName, leaseName)
		}
		return errors.New("lease-client-error")
	}
	t.Cleanup(func() { createKubernetesLease = oldCreateKubernetesLease })

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Nil(t, updatedMappings)
	assert.Contains(t, err.Error(), "lease-client-error")
	mockSE.AssertExpectations(t)
}

// Below test case will test that no k8's lease is getting created as LeaseName is already updated
func TestValidateAndCreateKubernetesLease(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node map with lease name already set
	nodeGroup := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-uuid-1"},
		Name:      "test-group-1",
		LeaseName: "harvest-test-lease-1",
	}

	// Create mapping with lease name already set in both NodeGroup and HarvestConfig
	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup,
			NodeGroupID:   nodeGroup.ID,
			HarvestConfig: &datamodel.HarvestConfig{LEASE_NAME: "harvest-test-lease-1"},
		},
	}

	// Mock the leaseExists function to return true (lease exists in Kubernetes)
	oldLeaseExists := leaseExists
	leaseExists = func(ctx context.Context, leaseNameSpace, leaseName string) (bool, error) {
		return true, nil // Lease exists in Kubernetes
	}
	defer func() { leaseExists = oldLeaseExists }()

	// No need to mock updates since lease already exists and no changes should be made
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		t.Fatal("createKubernetesLease should not be called when lease already exists")
		return nil
	}
	defer func() {
		createKubernetesLease = oldCreateKubernetesLease
	}()

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	assert.NotNil(t, updatedMappings)
	assert.Len(t, updatedMappings, len(nodeGroupsMap))
	for _, mapping := range updatedMappings {
		assert.NotNil(t, mapping.NodeGroup)
		assert.Equal(t, mapping.NodeGroup.LeaseName, mapping.HarvestConfig.LEASE_NAME)
	}
	mockSE.AssertExpectations(t)
}

// Below test case will test when GetNodeGroup call to DB fails
func TestValidateAndCreateKubernetesLease_DBError(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node map with proper initialization
	nodeGroup := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-uuid-1"},
		Name:      "test-group-1",
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup,
			NodeGroupID:   nodeGroup.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock DB error for UpdateNodeGroup - this should be called after lease creation
	mockSE.On("UpdateNodeGroup", ctx, mock.AnythingOfType("*datamodel.NodeGroup")).Return(nil, gorm.ErrRecordNotFound)

	// Override createKubernetesLease to return success since DB error is our test case
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	t.Cleanup(func() { createKubernetesLease = oldCreateKubernetesLease })

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Nil(t, updatedMappings)
	assert.Equal(t, "record not found", err.Error())
	mockSE.AssertExpectations(t)
}

// Tests the case where UpdateNodeNodeGroupMap fails after UpdateNodeGroup success
func TestValidateAndCreateKubernetesLease_UpdateNodeNodeGroupMapError(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	nodeGroup := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-uuid-1"},
		Name:      "test-group-1",
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup,
			NodeGroupID:   nodeGroup.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock UpdateNodeGroup to succeed
	mockSE.On("UpdateNodeGroup", ctx, mock.AnythingOfType("*datamodel.NodeGroup")).Return(nodeGroup, nil)

	// Mock UpdateNodeNodeGroupMap to fail
	mockSE.On("UpdateNodeNodeGroupMap", ctx, mock.AnythingOfType("*datamodel.NodeNodeGroupMap")).Return(nil, errors.New("failed to update node group map"))

	// Override createKubernetesLease to return success
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		return nil
	}
	t.Cleanup(func() { createKubernetesLease = oldCreateKubernetesLease })

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Nil(t, updatedMappings)
	assert.Contains(t, err.Error(), "failed to update node group map")

	// Verify lease name was correctly generated
	assert.Equal(t, "harvest-test-uuid-1", nodeGroup.LeaseName)
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

// Test case for when lease exists in database but not in Kubernetes
func TestValidateAndCreateKubernetesLease_LeaseExistsInDBButNotInK8s(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node group with existing lease name
	existingLeaseName := "harvest-existing-uuid"
	nodeGroup1 := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-uuid"},
		Name:      "test-group-1",
		LeaseName: existingLeaseName, // Lease name already exists in DB
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup1,
			NodeGroupID:   nodeGroup1.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock the leaseExists function to return false (lease doesn't exist in Kubernetes)
	oldLeaseExists := leaseExists
	leaseExists = func(ctx context.Context, leaseNameSpace, leaseName string) (bool, error) {
		assert.Equal(t, existingLeaseName, leaseName)
		return false, nil // Lease doesn't exist in Kubernetes
	}
	defer func() { leaseExists = oldLeaseExists }()

	// Mock createKubernetesLease to succeed
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		assert.Equal(t, existingLeaseName, leaseName)
		return nil
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	assert.NotNil(t, updatedMappings)
	assert.Len(t, updatedMappings, 1)
	assert.Equal(t, existingLeaseName, updatedMappings[0].HarvestConfig.LEASE_NAME)
}

// Test case for when lease exists in both database and Kubernetes
func TestValidateAndCreateKubernetesLease_LeaseExistsInBothDBAndK8s(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node group with existing lease name
	existingLeaseName := "harvest-existing-uuid"
	nodeGroup1 := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-uuid"},
		Name:      "test-group-1",
		LeaseName: existingLeaseName, // Lease name already exists in DB
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup1,
			NodeGroupID:   nodeGroup1.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock the leaseExists function to return true (lease exists in Kubernetes)
	oldLeaseExists := leaseExists
	leaseExists = func(ctx context.Context, leaseNameSpace, leaseName string) (bool, error) {
		assert.Equal(t, existingLeaseName, leaseName)
		return true, nil // Lease exists in Kubernetes
	}
	defer func() { leaseExists = oldLeaseExists }()

	// createKubernetesLease should not be called in this case
	oldCreateKubernetesLease := createKubernetesLease
	createKubernetesLease = func(ctx context.Context, leaseNameSpace, leaseName string) error {
		t.Error("createKubernetesLease should not be called when lease already exists")
		return nil
	}
	defer func() { createKubernetesLease = oldCreateKubernetesLease }()

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.NoError(t, err)
	assert.NotNil(t, updatedMappings)
	assert.Len(t, updatedMappings, 1)
	assert.Equal(t, existingLeaseName, updatedMappings[0].HarvestConfig.LEASE_NAME)
}

// Test case for when lease check fails
func TestValidateAndCreateKubernetesLease_LeaseCheckError(t *testing.T) {
	mockSE := new(database.MockStorage)
	ctx := context.Background()
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}

	// Create test node group with existing lease name
	existingLeaseName := "harvest-existing-uuid"
	nodeGroup1 := &datamodel.NodeGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "existing-uuid"},
		Name:      "test-group-1",
		LeaseName: existingLeaseName, // Lease name already exists in DB
	}

	nodeGroupsMap := []*datamodel.NodeNodeGroupMap{
		{
			BaseModel:     datamodel.BaseModel{ID: 1},
			NodeID:        1,
			NodeGroup:     nodeGroup1,
			NodeGroupID:   nodeGroup1.ID,
			HarvestConfig: &datamodel.HarvestConfig{},
		},
	}

	// Mock the leaseExists function to return an error
	oldLeaseExists := leaseExists
	leaseExists = func(ctx context.Context, leaseNameSpace, leaseName string) (bool, error) {
		return false, errors.New("kubernetes connection error")
	}
	defer func() { leaseExists = oldLeaseExists }()

	updatedMappings, err := activity.ValidateAndCreateKubernetesLease(ctx, nodeGroupsMap)
	assert.Error(t, err)
	assert.Nil(t, updatedMappings)
	assert.Contains(t, err.Error(), "kubernetes connection error")
}
