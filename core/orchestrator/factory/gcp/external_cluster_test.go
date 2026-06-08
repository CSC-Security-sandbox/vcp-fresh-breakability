package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestOnboardExternalClusters_RequiresHosts(t *testing.T) {
	orch := &GCPOrchestrator{storage: database.NewMockStorage(t)}

	tests := []struct {
		name   string
		params *commonparams.OnboardExternalClustersParams
	}{
		{name: "nil params", params: nil},
		{name: "empty hosts", params: &commonparams.OnboardExternalClustersParams{LocationID: "us-central1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := orch.OnboardExternalClusters(context.Background(), tt.params)
			require.Error(t, err)
			assert.True(t, utilserrors.IsBadRequestErr(err))
		})
	}
}

func TestOnboardExternalClusters_RequiresManagementIP(t *testing.T) {
	orch := &GCPOrchestrator{storage: database.NewMockStorage(t)}

	_, err := orch.OnboardExternalClusters(context.Background(), &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "  "},
		},
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.Contains(t, err.Error(), "managementIp is required")
}

func TestOnboardExternalClusters_EncryptPasswordError(t *testing.T) {
	originalEncrypt := utils.EncryptPassword
	t.Cleanup(func() { utils.EncryptPassword = originalEncrypt })
	utils.EncryptPassword = func(log.Secret) (*string, error) {
		return nil, errors.New("encrypt failed")
	}

	orch := &GCPOrchestrator{storage: database.NewMockStorage(t)}
	_, err := orch.OnboardExternalClusters(context.Background(), &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "10.0.0.1"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encrypt failed")
}

func TestOnboardExternalClusters_StorageCreateError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	mockStorage.On("CreateExternalCluster", mock.Anything, mock.Anything).
		Return(nil, errors.New("db insert failed"))

	_, err := orch.OnboardExternalClusters(context.Background(), &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "10.0.0.1"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db insert failed")
}

func TestGetExternalCluster_DelegatesToStorage(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	expected := &datamodel.Cluster{HostName: "host-1"}
	mockStorage.On("GetExternalCluster", mock.Anything, "uuid-1").Return(expected, nil)

	got, err := orch.GetExternalCluster(context.Background(), "uuid-1")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestDeleteExternalCluster_DelegatesToStorage(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	expected := &datamodel.Cluster{HostName: "host-1"}
	mockStorage.On("DeleteExternalCluster", mock.Anything, "uuid-1").Return(expected, nil)

	got, err := orch.DeleteExternalCluster(context.Background(), "uuid-1")
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestUpdateExternalCluster_NoUpdates(t *testing.T) {
	orch := &GCPOrchestrator{storage: database.NewMockStorage(t)}

	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{ExternalClusterID: "uuid-1"})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
}

func TestUpdateExternalCluster_GetHostError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	mockStorage.On("GetExternalCluster", mock.Anything, "uuid-1").Return(nil, errors.New("not found"))

	label := "new-label"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: "uuid-1",
		Label:             &label,
	})
	require.Error(t, err)
}

func TestUpdateExternalCluster_SetsManagementIPWhenAttributesNil(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{UUID: hostID},
		Protocol:  "HTTPS",
		Port:      443,
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.ClusterAttributes != nil && host.ClusterAttributes.ManagementIP == "10.1.2.3"
	})).Return(existing, nil)

	managementIP := "10.1.2.3"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		ManagementIP:      &managementIP,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_UpdatesLabelAndUsername(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel:     datamodel.BaseModel{UUID: hostID},
		Protocol:      "HTTPS",
		Port:          443,
		AdminUsername: "old-user",
		Label:         "old-label",
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Label == "new-label" && host.AdminUsername == "new-user"
	})).Return(existing, nil)

	label := "new-label"
	username := "new-user"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Label:             &label,
		Username:          &username,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_EncryptPasswordError(t *testing.T) {
	originalEncrypt := utils.EncryptPassword
	t.Cleanup(func() { utils.EncryptPassword = originalEncrypt })
	utils.EncryptPassword = func(log.Secret) (*string, error) {
		return nil, errors.New("encrypt failed")
	}

	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{UUID: hostID},
		Protocol:  "HTTPS",
		Port:      443,
	}
	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)

	password := "new-pass"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Password:          &password,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encrypt failed")
}

func TestUpdateExternalCluster_InvalidProtocol(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{UUID: hostID},
		Protocol:  "HTTPS",
		Port:      443,
	}
	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)

	protocol := "NFS"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Protocol:          &protocol,
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
}

func TestOnboardExternalClusters_StoresEncryptedAdminPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	const plaintext = "S3cr3tP@ssw0rd"
	params := &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: plaintext, ManagementIP: "10.0.0.1"},
		},
	}

	mockStorage.On("CreateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		if host.AdminPassword == "" || host.AdminPassword == plaintext {
			return false
		}
		decrypted, err := utils.DecryptPassword(log.Secret(host.AdminPassword))
		return err == nil && decrypted != nil && *decrypted == plaintext
	})).Return(&datamodel.Cluster{HostName: "host-1"}, nil)

	_, err := orch.OnboardExternalClusters(context.Background(), params)
	require.NoError(t, err)
}

func TestOnboardExternalClusters_ReturnedHostsOmitPlaintextPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	params := &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "plain-secret", ManagementIP: "10.0.0.1"},
		},
	}

	stored := &datamodel.Cluster{
		BaseModel:     datamodel.BaseModel{UUID: "uuid-1"},
		HostName:      "host-1",
		AdminUsername: "admin",
		AdminPassword: "v1:encrypted-blob",
	}
	mockStorage.On("CreateExternalCluster", mock.Anything, mock.Anything).Return(stored, nil)

	created, err := orch.OnboardExternalClusters(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, created, 1)
	assert.Equal(t, "v1:encrypted-blob", created[0].AdminPassword)
	assert.NotEqual(t, "plain-secret", created[0].AdminPassword)
}

func TestOnboardExternalClusters_PersistsDescriptionAndManagementIP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	params := &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{
				HostName:     "ontap-hw-cluster-01.example.com",
				Username:     "admin",
				Password:     "plain-secret",
				Description:  "Primary DR site",
				Label:        "type=SAPHANA",
				ManagementIP: "10.10.10.50",
			},
		},
	}

	mockStorage.On("CreateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		if host.ClusterAttributes == nil {
			return false
		}
		return host.Description == "Primary DR site" &&
			host.Label == "type=SAPHANA" &&
			host.ClusterAttributes.ManagementIP == "10.10.10.50" &&
			host.ClusterAttributes.OntapVersion == ""
	})).Return(&datamodel.Cluster{HostName: "ontap-hw-cluster-01.example.com"}, nil)

	_, err := orch.OnboardExternalClusters(context.Background(), params)
	require.NoError(t, err)
}

func TestOnboardExternalClusters_DefaultProtocolAndPort(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	params := &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "10.0.0.1"},
		},
	}

	mockStorage.On("CreateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Protocol == "INSECURE_HTTPS" && host.Port == 443
	})).Return(&datamodel.Cluster{HostName: "host-1"}, nil)

	_, err := orch.OnboardExternalClusters(context.Background(), params)
	require.NoError(t, err)
}

func TestOnboardExternalClusters_HTTPDefaultPort80(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	params := &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "10.0.0.1", Protocol: "HTTP"},
		},
	}

	mockStorage.On("CreateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Protocol == "HTTP" && host.Port == 80
	})).Return(&datamodel.Cluster{HostName: "host-1"}, nil)

	_, err := orch.OnboardExternalClusters(context.Background(), params)
	require.NoError(t, err)
}

func TestOnboardExternalClusters_InvalidProtocol(t *testing.T) {
	orch := &GCPOrchestrator{storage: database.NewMockStorage(t)}

	_, err := orch.OnboardExternalClusters(context.Background(), &commonparams.OnboardExternalClustersParams{
		LocationID: "us-central1",
		Hosts: []commonparams.ExternalClusterParams{
			{HostName: "host-1", Username: "admin", Password: "secret", ManagementIP: "10.0.0.1", Protocol: "NFS"},
		},
	})
	require.Error(t, err)
}

func TestUpdateExternalCluster_UpdatesDescriptionAndPreservesOntapVersion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel:     datamodel.BaseModel{UUID: hostID},
		Protocol:      "INSECURE_HTTPS",
		Port:          443,
		AdminUsername: "admin",
		AdminPassword: "v1:encrypted",
		ClusterAttributes: &datamodel.ClusterAttributes{
			ManagementIP: "10.0.0.1",
			OntapVersion: "9.15.1",
		},
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Description == "new desc" &&
			host.ClusterAttributes != nil &&
			host.ClusterAttributes.OntapVersion == "9.15.1" &&
			host.ClusterAttributes.ManagementIP == "10.0.0.1"
	})).Return(existing, nil)

	desc := "new desc"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Description:       &desc,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_RotatesPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel:     datamodel.BaseModel{UUID: hostID},
		Protocol:      "HTTPS",
		Port:          443,
		AdminUsername: "admin",
		AdminPassword: "v1:old",
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		if host.AdminPassword == "" || host.AdminPassword == "new-pass" {
			return false
		}
		decrypted, err := utils.DecryptPassword(log.Secret(host.AdminPassword))
		return err == nil && decrypted != nil && *decrypted == "new-pass"
	})).Return(existing, nil)

	password := "new-pass"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Password:          &password,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_ProtocolOnlyRedefaultsPort(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{UUID: hostID},
		Protocol:  "HTTPS",
		Port:      443,
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Protocol == "HTTP" && host.Port == 80
	})).Return(existing, nil)

	protocol := "HTTP"
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Protocol:          &protocol,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_PortOnlyKeepsProtocol(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel: datamodel.BaseModel{UUID: hostID},
		Protocol:  "HTTPS",
		Port:      443,
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.Protocol == "HTTPS" && host.Port == 8443
	})).Return(existing, nil)

	port := 8443
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		Port:              &port,
	})
	require.NoError(t, err)
}

func TestUpdateExternalCluster_ClearManagementIP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	orch := &GCPOrchestrator{storage: mockStorage}

	hostID := "uuid-1"
	existing := &datamodel.Cluster{
		BaseModel:         datamodel.BaseModel{UUID: hostID},
		Protocol:          "HTTPS",
		Port:              443,
		ClusterAttributes: &datamodel.ClusterAttributes{ManagementIP: "10.0.0.1"},
	}

	mockStorage.On("GetExternalCluster", mock.Anything, hostID).Return(existing, nil)
	mockStorage.On("UpdateExternalCluster", mock.Anything, mock.MatchedBy(func(host *datamodel.Cluster) bool {
		return host.ClusterAttributes != nil && host.ClusterAttributes.ManagementIP == ""
	})).Return(existing, nil)

	managementIP := ""
	_, err := orch.UpdateExternalCluster(context.Background(), &commonparams.UpdateExternalClusterParams{
		ExternalClusterID: hostID,
		ManagementIP:      &managementIP,
	})
	require.NoError(t, err)
}
