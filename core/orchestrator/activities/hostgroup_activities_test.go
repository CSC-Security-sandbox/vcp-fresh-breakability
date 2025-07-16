package activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestUpdateIGroups(t *testing.T) {
	t.Run("WhenUpdateIGroupsSuccessfulWithMultipleVolumesInSamePool", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { GetProviderByNode = _getProviderByNode }()

		mockStorage := database.NewMockStorage(t)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid",
			},
			Name: "test-pool",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		}

		volumes := []*datamodel.Volume{
			{
				Name: "volume-uuid1",
				Pool: pool,
				Svm: &datamodel.Svm{
					Name: "svm1",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"a1", "b1"},
							},
							{
								HostGroupUUID: "hg-uuid2",
								HostQNs:       []string{"a1", "b1"},
							},
						},
					},
				},
			},
			{
				Name: "volume-uuid2",
				Pool: pool,
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"b1"},
							},
						},
					},
				},
			},
		}

		mockStorage.On("GetAllVolumesForHG", ctx, mock.Anything, mock.Anything).Return(volumes, nil)

		nodes := []*datamodel.Node{{
			EndpointAddress: "1.1.1.1",
		}}

		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodes, nil)

		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := HostGroupUpdateActivity{
			SE: mockStorage,
		}
		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "hg-uuid1",
			},
			Name:        "hg1",
			Description: "test-description",
			Hosts: datamodel.Hosts{
				Hosts: []string{"c1", "d1"},
			},
			AccountID: 1,
		}

		IGroupOntap := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("1234"),
				IgroupInlineInitiators: []*ontapModels.IgroupInlineInitiatorsInlineArrayItem{{
					Name: nillable.GetStringPtr("a1"),
				}, {
					Name: nillable.GetStringPtr("b1"),
				},
				},
			},
		}
		mockProvider.On("IgroupExists", hg.Name, mock.Anything).Return(true, IGroupOntap, nil)

		mockProvider.On("IgroupAddInitiator", vsa.IgroupAddInitiator{
			Initiator:  []string{"c1", "d1"},
			IgroupUUID: "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "a1",
			IgroupUUID:    "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "b1",
			IgroupUUID:    "1234",
		}).Return(nil)

		mockStorage.On("UpdateVolume", ctx, mock.Anything).Return(nil)
		filter := utils.CreateFilterWithConditions(utils.NewFilterCondition("account_id", "=", hg.AccountID))
		mockStorage.On("ListPools", ctx, filter).Return([]*datamodel.PoolView{}, nil)

		err := activity.UpdateIGroups(ctx, hg)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenUpdateIGroupsSuccessfulWithMultiplePoolsWithoutVolume", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { GetProviderByNode = _getProviderByNode }()

		mockStorage := database.NewMockStorage(t)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		pool2 := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					UUID: "pool-uuid2",
				},
				Name: "test-pool2",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "abcd",
					SecretID:      "",
					CertificateID: "",
				},
			},
		}

		nodes := []*datamodel.Node{{
			EndpointAddress: "1.1.1.1",
		}}
		mockStorage.On("GetAllVolumesForHG", ctx, mock.Anything, mock.Anything).Return([]*datamodel.Volume{}, nil)

		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool2}, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodes, nil)

		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := HostGroupUpdateActivity{
			SE: mockStorage,
		}
		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "hg-uuid1",
			},
			Name:        "hg1",
			Description: "test-description",
			Hosts: datamodel.Hosts{
				Hosts: []string{"c1", "d1"},
			},
			AccountID: 1,
		}

		IGroupOntap := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("1234"),
				IgroupInlineInitiators: []*ontapModels.IgroupInlineInitiatorsInlineArrayItem{{
					Name: nillable.GetStringPtr("a1"),
				}, {
					Name: nillable.GetStringPtr("b1"),
				},
				},
			},
		}
		mockProvider.On("IgroupExists", hg.Name, mock.Anything).Return(true, IGroupOntap, nil)

		mockProvider.On("IgroupAddInitiator", vsa.IgroupAddInitiator{
			Initiator:  []string{"c1", "d1"},
			IgroupUUID: "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "a1",
			IgroupUUID:    "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "b1",
			IgroupUUID:    "1234",
		}).Return(nil)

		err := activity.UpdateIGroups(ctx, hg)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenUpdateIGroupsSuccessfulWithMultipleVolumesInDiffPool", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { GetProviderByNode = _getProviderByNode }()

		mockStorage := database.NewMockStorage(t)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		pool1 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid1",
			},
			Name: "test-pool1",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		}

		pool2 := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid2",
			},
			Name: "test-pool2",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		}

		volumes := []*datamodel.Volume{
			{
				Name: "volume-uuid1",
				Pool: pool1,
				Svm: &datamodel.Svm{
					Name: "svm1",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"a1", "b1"},
							},
						},
					},
				},
			},
			{
				Name: "volume-uuid2",
				Pool: pool2,
				Svm: &datamodel.Svm{
					Name: "svm1",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"a1", "b1"},
							},
						},
					},
				},
			},
		}

		mockStorage.On("GetAllVolumesForHG", ctx, mock.Anything, mock.Anything).Return(volumes, nil)

		nodes := []*datamodel.Node{{
			EndpointAddress: "1.1.1.1",
		}}

		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodes, nil)

		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := HostGroupUpdateActivity{
			SE: mockStorage,
		}
		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "hg-uuid1",
			},
			Name:        "hg1",
			Description: "test-description",
			Hosts: datamodel.Hosts{
				Hosts: []string{"c1", "d1"},
			},
			AccountID: 1,
		}

		IGroupOntap := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("1234"),
				IgroupInlineInitiators: []*ontapModels.IgroupInlineInitiatorsInlineArrayItem{{
					Name: nillable.GetStringPtr("a1"),
				}, {
					Name: nillable.GetStringPtr("b1"),
				},
				},
			},
		}
		mockProvider.On("IgroupExists", hg.Name, mock.Anything).Return(true, IGroupOntap, nil)

		mockProvider.On("IgroupAddInitiator", vsa.IgroupAddInitiator{
			Initiator:  []string{"c1", "d1"},
			IgroupUUID: "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "a1",
			IgroupUUID:    "1234",
		}).Return(nil)

		mockProvider.On("IgroupDeleteInitiator", vsa.IgroupDeleteInitiator{
			InitiatorName: "b1",
			IgroupUUID:    "1234",
		}).Return(nil)

		mockStorage.On("UpdateVolume", ctx, mock.Anything).Return(nil)

		err := activity.UpdateIGroups(ctx, hg)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenUpdateIGroupsAlreadySuccessfulWithMultipleVolumesInSamePool", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { GetProviderByNode = _getProviderByNode }()

		mockStorage := database.NewMockStorage(t)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid",
			},
			Name: "test-pool",
		}

		volumes := []*datamodel.Volume{
			{
				Name: "volume-uuid1",
				Pool: pool,
				Svm: &datamodel.Svm{
					Name: "svm1",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"a1", "b1"},
							},
						},
					},
				},
			},
		}

		mockStorage.On("GetAllVolumesForHG", ctx, mock.Anything, mock.Anything).Return(volumes, nil)
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := HostGroupUpdateActivity{
			SE: mockStorage,
		}
		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "hg-uuid1",
			},
			Name:        "hg1",
			Description: "test-description",
			Hosts: datamodel.Hosts{
				Hosts: []string{"a1", "b1"},
			},
			AccountID: 1,
		}

		err := activity.UpdateIGroups(ctx, hg)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
	t.Run("WhenUpdateIGroupsIsUpdatedOnOntap", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		defer func() { GetProviderByNode = _getProviderByNode }()

		mockStorage := database.NewMockStorage(t)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid",
			},
			Name: "test-pool",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		}

		volumes := []*datamodel.Volume{
			{
				Name: "volume-uuid1",
				Pool: pool,
				Svm: &datamodel.Svm{
					Name: "svm1",
				},
				VolumeAttributes: &datamodel.VolumeAttributes{
					ExternalUUID: "volume-uuid",
					BlockProperties: &datamodel.BlockProperties{
						HostGroupDetails: []datamodel.HostGroupDetail{
							{
								HostGroupUUID: "hg-uuid1",
								HostQNs:       []string{"c1", "b1"},
							},
						},
					},
				},
			},
		}

		mockStorage.On("GetAllVolumesForHG", ctx, mock.Anything, mock.Anything).Return(volumes, nil)
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{}, nil)
		nodes := []*datamodel.Node{{
			EndpointAddress: "1.1.1.1",
		}}

		mockStorage.On("GetNodesByPoolID", ctx, mock.Anything).Return(nodes, nil)

		GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := HostGroupUpdateActivity{
			SE: mockStorage,
		}
		hg := &datamodel.HostGroup{
			BaseModel: datamodel.BaseModel{
				UUID: "hg-uuid1",
			},
			Name:        "hg1",
			Description: "test-description",
			Hosts: datamodel.Hosts{
				Hosts: []string{"a1", "b1"},
			},
			AccountID: 1,
		}

		IGroupOntap := &ontap_rest.Igroup{
			Igroup: ontapModels.Igroup{
				UUID: nillable.GetStringPtr("1234"),
				IgroupInlineInitiators: []*ontapModels.IgroupInlineInitiatorsInlineArrayItem{{
					Name: nillable.GetStringPtr("a1"),
				}, {
					Name: nillable.GetStringPtr("b1"),
				},
				},
			},
		}
		mockProvider.On("IgroupExists", mock.Anything, mock.Anything).Return(true, IGroupOntap, nil)

		mockStorage.On("UpdateVolume", ctx, mock.Anything).Return(nil)

		err := activity.UpdateIGroups(ctx, hg)

		assert.NoError(t, err)
		mockProvider.AssertExpectations(t)
	})
}
