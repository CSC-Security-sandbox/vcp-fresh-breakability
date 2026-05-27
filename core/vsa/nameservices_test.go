package vsa

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestOntapRestProvider_CreateDns_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	provider := &OntapRestProvider{}

	params := CreateDnsParams{
		Domains: []string{"example.com"},
		Servers: []string{"8.8.8.8"},
	}
	mockClient.On("NameServices").Return(mockNameServices)
	mockNameServices.On("DnsCreate", &ontapRest.DNSCreateParams{
		Domains:    params.Domains,
		DNSServers: params.Servers,
	}).Return(nil, nil)

	err := provider.CreateDns(params)
	assert.NoError(t, err)
}

func TestOntapRestProvider_CreateDns_Failure(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	provider := &OntapRestProvider{}

	params := CreateDnsParams{
		Domains: []string{"example.com"},
		Servers: []string{"8.8.8.8"},
	}
	expectedErr := fmt.Errorf("API error")
	mockClient.On("NameServices").Return(mockNameServices)
	mockNameServices.On("DnsCreate", &ontapRest.DNSCreateParams{
		Domains:    params.Domains,
		DNSServers: params.Servers,
	}).Return(nil, expectedErr)

	err := provider.CreateDns(params)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

func TestOntapRestProvider_CreateLdap_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)
	mockSvm := new(ontapRest.MockSVMClient)
	mockNas := new(ontapRest.MockNASClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	volume := &datamodel.Volume{
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "svm-uuid",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"nfv3"},
		},
	}
	provider := &OntapRestProvider{}
	var builder strings.Builder
	domain := "example.com"
	for _, val := range strings.Split(domain, ".") {
		builder.WriteString("DC=")
		builder.WriteString(val)
		builder.WriteString(",")
	}

	var svms []*ontapRest.Svm
	var nsSwitchSources []*models.NsswitchSource
	nsSwitchDBValue := models.NsswitchSource("ldap")
	nsSwitchSources = append(nsSwitchSources, &nsSwitchDBValue)
	nsSwitch := &models.SvmInlineNsswitch{Group: nsSwitchSources, Hosts: []*models.NsswitchSource(nil), Namemap: nsSwitchSources, Netgroup: nsSwitchSources, Passwd: nsSwitchSources}
	svm := &ontapRest.Svm{
		Svm: models.Svm{
			Nsswitch: nsSwitch,
		},
	}
	svms = append(svms, svm)
	jobAccepted := &ontapRest.JobAccepted{
		JobUUID: "test-job-uuid",
	}

	mockClient.On("NameServices").Return(mockNameServices)
	mockClient.On("SVM").Return(mockSvm)
	mockClient.On("NAS").Return(mockNas)
	mockNas.On("NfsModify", mock.Anything).Return(nil)
	mockNameServices.On("LdapGet", &ontapRest.LdapGetParams{SvmUUID: volume.Svm.SvmDetails.ExternalUUID}).Return(nil, customerrors.NewNotFoundErr("Ldap", nil))
	mockNameServices.On("LdapSchemaCreate", mock.Anything).Return(nil, nil)
	mockNameServices.On("LdapSchemaModify", mock.Anything).Return(nil, nil)
	mockSvm.On("SvmCollectionGet", mock.Anything).Return(svms, nil)
	mockSvm.On("SvmModify", mock.Anything).Return(false, jobAccepted, nil)
	mockClient.On("Poll", mock.Anything).Return(nil)
	mockNas.On("NfsServiceModify", mock.Anything).Return(nil)

	ad := datamodel.ActiveDirectory{
		Domain: "example.com",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			UserDN:                        "user-example.com",
			GroupDN:                       "group-example.com",
			GroupMembershipFilter:         "membership-filter",
			PreferredServersForLdapClient: "10.0.1.150",
			AllowLocalNFSUsersWithLdap:    false,
		},
	}

	mockNameServices.On("LdapCreate", mock.Anything).Return(nil, nil)

	err := provider.CreateLdap(&ad, volume)
	assert.NoError(t, err)
}

func TestOntapRestProvider_CreateLdap_LdapSchemaExists(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)
	mockSvm := new(ontapRest.MockSVMClient)
	mockNas := new(ontapRest.MockNASClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	volume := &datamodel.Volume{
		Svm: &datamodel.Svm{
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "svm-uuid",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			Protocols: []string{"nfv3"},
		},
	}
	provider := &OntapRestProvider{}
	var builder strings.Builder
	domain := "example.com"
	for _, val := range strings.Split(domain, ".") {
		builder.WriteString("DC=")
		builder.WriteString(val)
		builder.WriteString(",")
	}

	var svms []*ontapRest.Svm
	var nsSwitchSources []*models.NsswitchSource
	nsSwitchDBValue := models.NsswitchSource("ldap")
	nsSwitchSources = append(nsSwitchSources, &nsSwitchDBValue)
	nsSwitch := &models.SvmInlineNsswitch{Group: nsSwitchSources, Hosts: []*models.NsswitchSource(nil), Namemap: nsSwitchSources, Netgroup: nsSwitchSources, Passwd: nsSwitchSources}
	svm := &ontapRest.Svm{
		Svm: models.Svm{
			Nsswitch: nsSwitch,
		},
	}
	svms = append(svms, svm)
	jobAccepted := &ontapRest.JobAccepted{
		JobUUID: "test-job-uuid",
	}

	mockClient.On("NameServices").Return(mockNameServices)
	mockClient.On("SVM").Return(mockSvm)
	mockClient.On("NAS").Return(mockNas)
	mockNas.On("NfsModify", mock.Anything).Return(nil)
	mockNameServices.On("LdapGet", &ontapRest.LdapGetParams{SvmUUID: volume.Svm.SvmDetails.ExternalUUID}).Return(nil, customerrors.NewNotFoundErr("Ldap", nil))
	mockNameServices.On("LdapSchemaCreate", mock.Anything).Return(errors.New("duplicate entry"))
	mockNameServices.On("LdapSchemaModify", mock.Anything).Return(nil, nil)
	mockSvm.On("SvmCollectionGet", mock.Anything).Return(svms, nil)
	mockSvm.On("SvmModify", mock.Anything).Return(false, jobAccepted, nil)
	mockClient.On("Poll", mock.Anything).Return(nil)
	mockNas.On("NfsServiceModify", mock.Anything).Return(nil)

	ad := datamodel.ActiveDirectory{
		Domain: "example.com",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			UserDN:                        "user-example.com",
			GroupDN:                       "group-example.com",
			GroupMembershipFilter:         "membership-filter",
			PreferredServersForLdapClient: "10.0.1.150",
			AllowLocalNFSUsersWithLdap:    false,
		},
	}

	mockNameServices.On("LdapCreate", mock.Anything).Return(nil, nil)

	err := provider.CreateLdap(&ad, volume)
	assert.NoError(t, err)
}

func TestOntapRestProvider_DeleteLdap_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)
	mockSvm := new(ontapRest.MockSVMClient)

	provider := &OntapRestProvider{}
	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	var svms []*ontapRest.Svm
	var nsSwitchSources []*models.NsswitchSource
	nsSwitchDBValue := models.NsswitchSource("ldap")
	nsSwitchSources = append(nsSwitchSources, &nsSwitchDBValue)
	nsSwitch := &models.SvmInlineNsswitch{Group: nsSwitchSources, Hosts: []*models.NsswitchSource(nil), Namemap: nsSwitchSources, Netgroup: nsSwitchSources, Passwd: nsSwitchSources}
	svm := &ontapRest.Svm{
		Svm: models.Svm{
			Nsswitch: nsSwitch,
		},
	}
	svms = append(svms, svm)
	jobAccepted := &ontapRest.JobAccepted{
		JobUUID: "test-job-uuid",
	}

	mockClient.On("NameServices").Return(mockNameServices)
	mockClient.On("SVM").Return(mockSvm)
	mockSvm.On("SvmCollectionGet", mock.Anything).Return(svms, nil)
	mockSvm.On("SvmModify", mock.Anything).Return(false, jobAccepted, nil)
	mockClient.On("Poll", mock.Anything).Return(nil)

	svmUUID := "test-svm-uuid"

	mockNameServices.On("LdapDelete", mock.Anything).Return(nil)

	err := provider.DeleteLdap(svmUUID)
	assert.NoError(t, err)
}
