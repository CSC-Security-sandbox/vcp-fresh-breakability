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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
)

func TestRegisterNodeToHarvestFarm_Success(t *testing.T) {
	mockSE := new(database.MockStorage)
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", PoolID: 42}, {BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", PoolID: 42}}
	maps := []*datamodel.NodeNodeGroupMap{
		{HarvestConfig: &datamodel.HarvestConfig{}},
		{HarvestConfig: &datamodel.HarvestConfig{}},
	}
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(42)).Return(nodes, nil)
	mockSE.On("AssignTwoNodesToTwoGroups", mock.Anything, nodes[0], nodes[1], 10).Return(maps, nil)
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	result, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 42, MaxNodesPerGroup: 10})
	assert.NoError(t, err)
	assert.Equal(t, maps, result)
}

func TestRegisterNodeToHarvestFarm_NoNodes(t *testing.T) {
	mockSE := new(database.MockStorage)
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return([]*datamodel.Node{}, nil)
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5})
	assert.Error(t, err)
}

func TestRegisterNodeToHarvestFarm_DBError(t *testing.T) {
	mockSE := new(database.MockStorage)
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nil, errors.New("db error"))
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5})
	assert.Error(t, err)
}

func TestRegisterNodeToHarvestFarm_AssignError(t *testing.T) {
	mockSE := new(database.MockStorage)
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", PoolID: 1}, {BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", PoolID: 1}}
	mockSE.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockSE.On("AssignTwoNodesToTwoGroups", mock.Anything, nodes[0], nodes[1], 5).Return(nil, errors.New("assign error"))
	activity := &RegisterNodeToHarvestFarmActivity{SE: mockSE}
	ctx := context.Background()
	_, err := activity.RegisterNodeToHarvestFarm(ctx, RegisterNodeToHarvestFarmInput{PoolID: 1, MaxNodesPerGroup: 5})
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
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}}},
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
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}}},
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
		NodeMappings: []*datamodel.NodeNodeGroupMap{{HarvestConfig: &datamodel.HarvestConfig{}}},
		UploadURL:    "http://localhost:0", // invalid port
	}
	activity := &UploadHarvestTemplateActivity{
		LoadHarvestTemplateFunc:   func() (string, error) { return "template", nil },
		RenderHarvestTemplateFunc: func(cfg *datamodel.HarvestConfig) (string, error) { return "fake-yaml", nil },
	}
	ctx := context.Background()
	assert.Error(t, activity.UploadHarvestTemplate(ctx, input))
}
