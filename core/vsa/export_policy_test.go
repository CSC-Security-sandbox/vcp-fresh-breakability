package vsa

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// MockNASClient is a mock for the NASClient interface
type MockNASClient struct {
	mock.Mock
}

func (m *MockNASClient) ExportPolicyCreate(params *ontapRest.ExportPolicyCreateParams) (string, error) {
	args := m.Called(params)
	return args.String(0), args.Error(1)
}

func (m *MockNASClient) ExportPolicyGet(params *ontapRest.ExportPolicyGetParams) (*ontapRest.ExportPolicy, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ontapRest.ExportPolicy), args.Error(1)
}

func (m *MockNASClient) ExportPoliciesGet(params *ontapRest.ExportPolicyGetParams) ([]*ontapRest.ExportPolicy, error) {
	args := m.Called(params)
	return args.Get(0).([]*ontapRest.ExportPolicy), args.Error(1)
}

func (m *MockNASClient) ExportPolicyModify(params *ontapRest.ExportPolicyModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) ExportPolicyDelete(params *ontapRest.ExportPolicyDeleteParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) NfsServiceGet(params *ontapRest.NfsServiceGetParams) (*ontapRest.NfsService, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ontapRest.NfsService), args.Error(1)
}

func (m *MockNASClient) NfsServiceCreate(params *ontapRest.NfsServiceCreateParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) NfsServiceModify(params *ontapRest.NfsServiceModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) NfsParamsModify(ctx context.Context, params *ontapRest.NfsModifyParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceGet(params *ontapRest.CifsServiceGetParams) (*ontapRest.CifsService, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ontapRest.CifsService), args.Error(1)
}

func (m *MockNASClient) CifsServiceList(params *ontapRest.CifsServiceGetParams) ([]*ontapRest.CifsService, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*ontapRest.CifsService), args.Error(1)
}

func (m *MockNASClient) CifsServiceCreate(params *ontapRest.CifsServiceCreateParams) (bool, *ontapRest.JobAccepted, error) {
	args := m.Called(params)
	var job *ontapRest.JobAccepted
	if val := args.Get(1); val != nil {
		job = val.(*ontapRest.JobAccepted)
	}
	return args.Bool(0), job, args.Error(2)
}

func (m *MockNASClient) CifsServiceModify(params *ontapRest.CifsServiceModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsDomainModify(params *ontapRest.CifsDomainModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsDomainGet(params *ontapRest.CifsDomainGetParams) (*ontapRest.CifsDomain, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ontapRest.CifsDomain), args.Error(1)
}

func (m *MockNASClient) CifsShareACLDelete(params *ontapRest.CifsShareACLDeleteParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceAddMembers(params *ontapRest.CifsServiceModifyGroupMembersParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceDelete(params *ontapRest.CifsServiceDeleteParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceAddSecurityPrivilege(params *ontapRest.CifsServiceModifySecurityPrivilegeParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsShareCreate(params *ontapRest.CifsShareCreateParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsShareModify(params *ontapRest.CifsShareModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsShareCollectionGet(params *ontapRest.CifsShareCollectionGetParams) (*ontapRest.CifsShareGetResponse, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ontapRest.CifsShareGetResponse), args.Error(1)
}

func (m *MockNASClient) DomainControllersSrvLookupGet(params *ontapRest.SrvLookupParams) ([]string, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockNASClient) CifsDomainPreferredDCDelete(params *ontapRest.CifsDomainPreferredDCDeleteParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsDomainPreferredDCCreate(params *ontapRest.CifsDomainPreferredDCCreateParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceCollectionGetGroups(params *ontapRest.CifsServiceCollectionGetGroupsParams, ucbf ontapRest.UserCallbackFunc[[]*ontapRest.CifsGroup]) error {
	args := m.Called(params, ucbf)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceRemoveMembers(params *ontapRest.CifsServiceModifyGroupMembersParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceCollectionGetPrivilegedMembers(params *ontapRest.CifsServiceCollectionGetPrivilegedMembersParams, ucbf ontapRest.UserCallbackFunc[[]string]) error {
	args := m.Called(params, ucbf)
	return args.Error(0)
}

func (m *MockNASClient) CifsServiceRemoveSecurityPrivilege(params *ontapRest.CifsServiceModifySecurityPrivilegeParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) NfsModify(params *ontapRest.NfsModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) KerberosRealmGet(params *ontapRest.KerberosRealmGetParams) ([]*ontapRest.KerberosRealm, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*ontapRest.KerberosRealm), args.Error(1)
}

func (m *MockNASClient) KerberosRealmCreate(params *ontapRest.KerberosRealmCreateParams) error {
	args := m.Called(params)
	return args.Error(0)
}

func (m *MockNASClient) KerberosInterfaceCollectionGet(params *ontapRest.KerberosInterfaceCollectionGetParams) ([]*ontapRest.KerberosInterface, error) {
	args := m.Called(params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*ontapRest.KerberosInterface), args.Error(1)
}

func (m *MockNASClient) KerberosInterfaceModify(params *ontapRest.KerberosInterfaceModifyParams) error {
	args := m.Called(params)
	return args.Error(0)
}

// MockRESTClientForNAS extends the existing MockRESTClient to include NAS
type MockRESTClientForNAS struct {
	ontapRest.MockRESTClient
	nasClient     *MockNASClient
	storageClient *ontapRest.MockStorageClient
}

func (m *MockRESTClientForNAS) NAS() ontapRest.NASClient {
	return m.nasClient
}

func (m *MockRESTClientForNAS) Storage() ontapRest.StorageClient {
	return m.storageClient
}

func TestConvertStorageExportPolicyRuleToONTAP(t *testing.T) {
	tests := []struct {
		name     string
		rule     ExportRule
		index    int
		expected *ontapRest.ExportRule
	}{
		{
			name: "CIFS and NFSv3 enabled with superuser",
			rule: ExportRule{
				AllowedClients: "192.168.1.0/24",
				AnonymousUser:  "",
				Index:          1,
				ChownMode:      "",
				CIFS:           true,
				NFSv3:          true,
				NFSv4:          false,
				Superuser:      true,
			},
			index: 1,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "192.168.1.0/24",
				ChownMode:        models.ChownModeRestricted,
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.AnyAccessProtocol,
				Index:            1,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv3)},
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
		{
			name: "NFSv4 only without superuser",
			rule: ExportRule{
				AllowedClients: "10.0.0.0/8",
				AnonymousUser:  "nobody",
				Index:          2,
				ChownMode:      "unrestricted",
				CIFS:           false,
				NFSv3:          false,
				NFSv4:          true,
				Superuser:      false,
			},
			index: 2,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "10.0.0.0/8",
				ChownMode:        "unrestricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            2,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolNFSv4)},
				AnonymousUser:    "nobody",
			},
		},
		{
			name: "No protocols enabled",
			rule: ExportRule{
				AllowedClients: "0.0.0.0/0",
				AnonymousUser:  "",
				Index:          3,
				ChownMode:      "",
				CIFS:           false,
				NFSv3:          false,
				NFSv4:          false,
				Superuser:      false,
			},
			index: 3,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "0.0.0.0/0",
				ChownMode:        models.ChownModeRestricted,
				ReadOnlyRule:     models.ExportAuthenticationFlavorNever,
				ReadWriteRule:    models.ExportAuthenticationFlavorNever,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            3,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        nil,
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
		{
			name: "All protocols enabled",
			rule: ExportRule{
				AllowedClients: "172.16.0.0/16",
				AnonymousUser:  "guest",
				Index:          4,
				ChownMode:      "restricted",
				CIFS:           true,
				NFSv3:          true,
				NFSv4:          true,
				Superuser:      true,
			},
			index: 4,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "172.16.0.0/16",
				ChownMode:        "restricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.AnyAccessProtocol,
				Index:            4,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv3), utils.GetOntapValue(utils.ProtocolNFSv4)},
				AnonymousUser:    "guest",
			},
		},
		{
			name: "Only CIFS enabled",
			rule: ExportRule{
				AllowedClients: "192.168.100.0/24",
				AnonymousUser:  "admin",
				Index:          5,
				ChownMode:      "unrestricted",
				CIFS:           true,
				NFSv3:          false,
				NFSv4:          false,
				Superuser:      false,
			},
			index: 5,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "192.168.100.0/24",
				ChownMode:        "unrestricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            5,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB)},
				AnonymousUser:    "admin",
			},
		},
		{
			name: "CIFS and NFSv4 combination",
			rule: ExportRule{
				AllowedClients: "10.10.10.0/24",
				AnonymousUser:  "",
				Index:          6,
				ChownMode:      "",
				CIFS:           true,
				NFSv3:          false,
				NFSv4:          true,
				Superuser:      true,
			},
			index: 6,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "10.10.10.0/24",
				ChownMode:        models.ChownModeRestricted,
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.AnyAccessProtocol,
				Index:            6,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv4)},
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
		{
			name: "NFSv3 and NFSv4 combination without CIFS",
			rule: ExportRule{
				AllowedClients: "172.20.0.0/16",
				AnonymousUser:  "testuser",
				Index:          7,
				ChownMode:      "restricted",
				CIFS:           false,
				NFSv3:          true,
				NFSv4:          true,
				Superuser:      false,
			},
			index: 7,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "172.20.0.0/16",
				ChownMode:        "restricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            7,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolNFSv3), utils.GetOntapValue(utils.ProtocolNFSv4)},
				AnonymousUser:    "testuser",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertStorageExportPolicyRuleToONTAP(tt.rule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDefaultRule(t *testing.T) {
	tests := []struct {
		name     string
		rule     *ontaprestmodels.ExportRules
		expected bool
	}{
		{
			name: "Valid default rule",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: true,
		},
		{
			name: "Wrong client match",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("192.168.1.0/24")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong index",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(1)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Multiple clients",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
					{Match: nillable.ToPointer("192.168.1.0/24")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong chown mode",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("unrestricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong protocol",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("cifs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Multiple protocols",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs"), nillable.ToPointer("cifs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong ro rule",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorSys))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong rw rule",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorSys))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Wrong superuser rule",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorSys))},
			},
			expected: false,
		},
		{
			name: "Multiple ro rules",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone)), (*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorSys))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "No clients",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients:   []*ontaprestmodels.ExportClients{},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDefaultRule(tt.rule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportPolicyEnsureDefault_Success_DefaultRuleExists(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}
	rc := &OntapRestProvider{Logger: log.NewLogger()}
	svmName := "testSVM"

	// Mock export policy with default rule
	defaultRule := &ontaprestmodels.ExportRules{
		ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
			{Match: nillable.ToPointer("0.0.0.0/0")},
		},
		Index:                      nillable.ToPointer(int64(7)),
		ChownMode:                  nillable.ToPointer("restricted"),
		Protocols:                  []*string{nillable.ToPointer("nfs")},
		ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
	}

	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == svmName
	})).Return(mockExportPolicy, nil)

	err := rc.ExportPolicyEnsureDefault(svmName)

	assert.NoError(t, err)
	mockNASClient.AssertExpectations(t)
}

func TestExportPolicyEnsureDefault_Success_CreatesDefaultRule(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}
	svmName := "testSVM"

	// Mock export policy without default rule
	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == svmName
	})).Return(mockExportPolicy, nil)

	mockNASClient.On("ExportPolicyModify", mock.MatchedBy(func(params *ontapRest.ExportPolicyModifyParams) bool {
		return params.SvmName == svmName &&
			params.ID == 123 &&
			len(params.Rules) == 1 &&
			params.Rules[0].Index == 7 &&
			params.Rules[0].ChownMode == models.ChownModeRestricted
	})).Return(nil)

	err := rc.ExportPolicyEnsureDefault(svmName)

	assert.NoError(t, err)
	mockNASClient.AssertExpectations(t)
}

func TestExportPolicyEnsureDefault_Error_PolicyNotFound(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}
	svmName := "testSVM"

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == svmName
	})).Return(nil, nil)

	err := rc.ExportPolicyEnsureDefault(svmName)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Export policy")
	mockNASClient.AssertExpectations(t)
}

func TestExportPolicyEnsureDefault_Error_GetFails(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}
	svmName := "testSVM"

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == svmName
	})).Return(nil, errors.New("API error"))

	err := rc.ExportPolicyEnsureDefault(svmName)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "API error")
	mockNASClient.AssertExpectations(t)
}

func TestExportPolicyEnsureDefault_Error_ModifyFails(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}
	svmName := "testSVM"

	// Mock export policy without default rule
	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == svmName
	})).Return(mockExportPolicy, nil)

	mockNASClient.On("ExportPolicyModify", mock.Anything).Return(errors.New("modify error"))

	err := rc.ExportPolicyEnsureDefault(svmName)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "modify error")
	mockNASClient.AssertExpectations(t)
}

func TestCreateExportPolicy_Success(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
		ExportRules: []*ExportRule{
			{
				AllowedClients: "192.168.1.0/24",
				Index:          1,
				CIFS:           true,
				NFSv3:          true,
				Superuser:      true,
			},
			{
				AllowedClients: "10.0.0.0/8",
				Index:          2,
				NFSv4:          true,
				Superuser:      false,
			},
		},
	}

	// Mock ensuring default export policy
	defaultRule := &ontaprestmodels.ExportRules{
		ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
			{Match: nillable.ToPointer("0.0.0.0/0")},
		},
		Index:                      nillable.ToPointer(int64(7)),
		ChownMode:                  nillable.ToPointer("restricted"),
		Protocols:                  []*string{nillable.ToPointer("nfs")},
		ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
	}

	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == exportPolicy.SvmName
	})).Return(mockExportPolicy, nil)

	mockNASClient.On("ExportPolicyCreate", mock.MatchedBy(func(params *ontapRest.ExportPolicyCreateParams) bool {
		return params.Name == exportPolicy.ExportPolicyName &&
			params.SvmName == exportPolicy.SvmName &&
			len(params.Rules) == 2
	})).Return("new-policy-id", nil)

	err := rc.CreateExportPolicy(exportPolicy)

	assert.NoError(t, err)
	mockNASClient.AssertExpectations(t)
}

func TestCreateExportPolicy_Error_EnsureDefaultFails(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
		ExportRules: []*ExportRule{
			{
				AllowedClients: "192.168.1.0/24",
				Index:          1,
				CIFS:           true,
			},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == exportPolicy.SvmName
	})).Return(nil, errors.New("default policy error"))

	err := rc.CreateExportPolicy(exportPolicy)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to ensure default export policy")
	assert.Contains(t, err.Error(), "default policy error")
	mockNASClient.AssertExpectations(t)
}

func TestCreateExportPolicy_Error_CreateFails(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
		ExportRules: []*ExportRule{
			{
				AllowedClients: "192.168.1.0/24",
				Index:          1,
				CIFS:           true,
			},
		},
	}

	// Mock ensuring default export policy succeeds
	defaultRule := &ontaprestmodels.ExportRules{
		ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
			{Match: nillable.ToPointer("0.0.0.0/0")},
		},
		Index:                      nillable.ToPointer(int64(7)),
		ChownMode:                  nillable.ToPointer("restricted"),
		Protocols:                  []*string{nillable.ToPointer("nfs")},
		ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
	}

	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == exportPolicy.SvmName
	})).Return(mockExportPolicy, nil)

	mockNASClient.On("ExportPolicyCreate", mock.Anything).Return("", errors.New("create error"))

	err := rc.CreateExportPolicy(exportPolicy)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "create error")
	mockNASClient.AssertExpectations(t)
}

func TestCreateExportPolicy_Success_EmptyRules(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
		ExportRules:      []*ExportRule{},
	}

	// Mock ensuring default export policy
	defaultRule := &ontaprestmodels.ExportRules{
		ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
			{Match: nillable.ToPointer("0.0.0.0/0")},
		},
		Index:                      nillable.ToPointer(int64(7)),
		ChownMode:                  nillable.ToPointer("restricted"),
		Protocols:                  []*string{nillable.ToPointer("nfs")},
		ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
		ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
	}

	mockExportPolicy := &ontapRest.ExportPolicy{
		ExportPolicy: ontaprestmodels.ExportPolicy{
			ID:                      nillable.ToPointer(int64(123)),
			ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
		},
	}

	mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
		return *params.Name == models.DefaultExportPolicyName && *params.SvmName == exportPolicy.SvmName
	})).Return(mockExportPolicy, nil)

	mockNASClient.On("ExportPolicyCreate", mock.MatchedBy(func(params *ontapRest.ExportPolicyCreateParams) bool {
		return params.Name == exportPolicy.ExportPolicyName &&
			params.SvmName == exportPolicy.SvmName &&
			len(params.Rules) == 0
	})).Return("new-policy-id", nil)

	err := rc.CreateExportPolicy(exportPolicy)

	assert.NoError(t, err)
	mockNASClient.AssertExpectations(t)
}

// Additional comprehensive edge case tests
func TestConvertStorageExportPolicyRuleToONTAP_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		rule     ExportRule
		index    int
		expected *ontapRest.ExportRule
	}{
		{
			name: "Only S3 protocol enabled (should result in no protocols)",
			rule: ExportRule{
				AllowedClients: "192.168.1.0/24",
				AnonymousUser:  "",
				Index:          1,
				ChownMode:      "",
				CIFS:           false,
				NFSv3:          false,
				NFSv4:          false,
				S3:             true, // S3 is not handled in the current logic
				Superuser:      false,
			},
			index: 1,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "192.168.1.0/24",
				ChownMode:        models.ChownModeRestricted,
				ReadOnlyRule:     models.ExportAuthenticationFlavorNever,
				ReadWriteRule:    models.ExportAuthenticationFlavorNever,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            1,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        nil,
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
		{
			name: "Large index value",
			rule: ExportRule{
				AllowedClients: "0.0.0.0/0",
				AnonymousUser:  "nobody",
				Index:          99999,
				ChownMode:      "restricted",
				CIFS:           true,
				NFSv3:          false,
				NFSv4:          false,
				Superuser:      true,
			},
			index: 99999,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "0.0.0.0/0",
				ChownMode:        "restricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.AnyAccessProtocol,
				Index:            99999,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB)},
				AnonymousUser:    "nobody",
			},
		},
		{
			name: "Empty client match",
			rule: ExportRule{
				AllowedClients: "",
				AnonymousUser:  "",
				Index:          1,
				ChownMode:      "",
				CIFS:           true,
				NFSv3:          false,
				NFSv4:          false,
				Superuser:      false,
			},
			index: 1,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "",
				ChownMode:        models.ChownModeRestricted,
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            1,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolSMB)},
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
		{
			name: "Custom chown mode override",
			rule: ExportRule{
				AllowedClients: "192.168.1.0/24",
				AnonymousUser:  "",
				Index:          1,
				ChownMode:      "unrestricted",
				CIFS:           false,
				NFSv3:          true,
				NFSv4:          false,
				Superuser:      false,
			},
			index: 1,
			expected: &ontapRest.ExportRule{
				ClientMatch:      "192.168.1.0/24",
				ChownMode:        "unrestricted",
				ReadOnlyRule:     models.ExportAuthenticationFlavorSys,
				ReadWriteRule:    models.AnyAccessProtocol,
				SuperUserRule:    models.NoneAccessProtocol,
				Index:            1,
				NtfsUnixSecurity: *nillable.ToPointer("ignore"),
				Protocols:        []string{utils.GetOntapValue(utils.ProtocolNFSv3)},
				AnonymousUser:    models.RootAnonymousUser,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertStorageExportPolicyRuleToONTAP(tt.rule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsDefaultRule_ExtensiveEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		rule     *ontaprestmodels.ExportRules
		expected bool
	}{
		{
			name: "Nil clients slice",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients:   nil,
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Empty clients slice",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients:   []*ontaprestmodels.ExportClients{},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Nil protocols slice",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  nil,
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Empty protocols slice",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
			},
			expected: false,
		},
		{
			name: "Nil authentication rule slices",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    nil,
				ExportRulesInlineRwRule:    nil,
				ExportRulesInlineSuperuser: nil,
			},
			expected: false,
		},
		{
			name: "Empty authentication rule slices",
			rule: &ontaprestmodels.ExportRules{
				ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
					{Match: nillable.ToPointer("0.0.0.0/0")},
				},
				Index:                      nillable.ToPointer(int64(7)),
				ChownMode:                  nillable.ToPointer("restricted"),
				Protocols:                  []*string{nillable.ToPointer("nfs")},
				ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{},
				ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{},
				ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDefaultRule(tt.rule)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExportPolicyEnsureDefault_ErrorEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*MockNASClient)
		wantErr bool
		errMsg  string
	}{
		{
			name: "Export policy has multiple rules but no default rule",
			setup: func(mockNASClient *MockNASClient) {
				nonDefaultRule := &ontaprestmodels.ExportRules{
					ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
						{Match: nillable.ToPointer("192.168.1.0/24")},
					},
					Index:                      nillable.ToPointer(int64(1)),
					ChownMode:                  nillable.ToPointer("restricted"),
					Protocols:                  []*string{nillable.ToPointer("nfs")},
					ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				}

				mockExportPolicy := &ontapRest.ExportPolicy{
					ExportPolicy: ontaprestmodels.ExportPolicy{
						ID:                      nillable.ToPointer(int64(123)),
						ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{nonDefaultRule},
					},
				}

				mockNASClient.On("ExportPolicyGet", mock.Anything).Return(mockExportPolicy, nil)
				mockNASClient.On("ExportPolicyModify", mock.Anything).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "Export policy has multiple rules with wrong default rule",
			setup: func(mockNASClient *MockNASClient) {
				wrongDefaultRule := &ontaprestmodels.ExportRules{
					ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
						{Match: nillable.ToPointer("0.0.0.0/0")},
					},
					Index:                      nillable.ToPointer(int64(7)),
					ChownMode:                  nillable.ToPointer("unrestricted"), // Wrong chown mode
					Protocols:                  []*string{nillable.ToPointer("nfs")},
					ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				}

				anotherRule := &ontaprestmodels.ExportRules{
					ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
						{Match: nillable.ToPointer("192.168.1.0/24")},
					},
					Index:                      nillable.ToPointer(int64(1)),
					ChownMode:                  nillable.ToPointer("restricted"),
					Protocols:                  []*string{nillable.ToPointer("nfs")},
					ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				}

				mockExportPolicy := &ontapRest.ExportPolicy{
					ExportPolicy: ontaprestmodels.ExportPolicy{
						ID:                      nillable.ToPointer(int64(123)),
						ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{wrongDefaultRule, anotherRule},
					},
				}

				mockNASClient.On("ExportPolicyGet", mock.Anything).Return(mockExportPolicy, nil)
				mockNASClient.On("ExportPolicyModify", mock.Anything).Return(nil)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNASClient := new(MockNASClient)
			mockRESTClient := &MockRESTClientForNAS{
				nasClient: mockNASClient,
			}

			getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockRESTClient, nil
			}

			rc := &OntapRestProvider{Logger: log.NewLogger()}
			svmName := "testSVM"

			tt.setup(mockNASClient)

			err := rc.ExportPolicyEnsureDefault(svmName)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			mockNASClient.AssertExpectations(t)
		})
	}
}

func TestCreateExportPolicy_ExtensiveEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		policy  *ExportPolicy
		setup   func(*MockNASClient)
		wantErr bool
		errMsg  string
	}{
		{
			name: "Export policy with nil export rules",
			policy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				SvmName:          "testSVM",
				ExportRules:      nil,
			},
			setup: func(mockNASClient *MockNASClient) {
				// Mock ensuring default export policy
				defaultRule := &ontaprestmodels.ExportRules{
					ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
						{Match: nillable.ToPointer("0.0.0.0/0")},
					},
					Index:                      nillable.ToPointer(int64(7)),
					ChownMode:                  nillable.ToPointer("restricted"),
					Protocols:                  []*string{nillable.ToPointer("nfs")},
					ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				}

				mockExportPolicy := &ontapRest.ExportPolicy{
					ExportPolicy: ontaprestmodels.ExportPolicy{
						ID:                      nillable.ToPointer(int64(123)),
						ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
					},
				}

				mockNASClient.On("ExportPolicyGet", mock.Anything).Return(mockExportPolicy, nil)
				mockNASClient.On("ExportPolicyCreate", mock.MatchedBy(func(params *ontapRest.ExportPolicyCreateParams) bool {
					return params.Name == "test-policy" &&
						params.SvmName == "testSVM" &&
						len(params.Rules) == 0
				})).Return("new-policy-id", nil)
			},
			wantErr: false,
		},
		{
			name: "Export policy with large number of rules",
			policy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				SvmName:          "testSVM",
				ExportRules: func() []*ExportRule {
					rules := make([]*ExportRule, 100)
					for i := 0; i < 100; i++ {
						rules[i] = &ExportRule{
							AllowedClients: fmt.Sprintf("192.168.%d.0/24", i+1),
							Index:          i + 1,
							CIFS:           i%2 == 0,
							NFSv3:          i%3 == 0,
							NFSv4:          i%4 == 0,
							Superuser:      i%5 == 0,
						}
					}
					return rules
				}(),
			},
			setup: func(mockNASClient *MockNASClient) {
				// Mock ensuring default export policy
				defaultRule := &ontaprestmodels.ExportRules{
					ExportRulesInlineClients: []*ontaprestmodels.ExportClients{
						{Match: nillable.ToPointer("0.0.0.0/0")},
					},
					Index:                      nillable.ToPointer(int64(7)),
					ChownMode:                  nillable.ToPointer("restricted"),
					Protocols:                  []*string{nillable.ToPointer("nfs")},
					ExportRulesInlineRoRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineRwRule:    []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
					ExportRulesInlineSuperuser: []*ontaprestmodels.ExportAuthenticationFlavor{(*ontaprestmodels.ExportAuthenticationFlavor)(nillable.ToPointer(ontaprestmodels.ExportAuthenticationFlavorNone))},
				}

				mockExportPolicy := &ontapRest.ExportPolicy{
					ExportPolicy: ontaprestmodels.ExportPolicy{
						ID:                      nillable.ToPointer(int64(123)),
						ExportPolicyInlineRules: []*ontaprestmodels.ExportRules{defaultRule},
					},
				}

				mockNASClient.On("ExportPolicyGet", mock.Anything).Return(mockExportPolicy, nil)
				mockNASClient.On("ExportPolicyCreate", mock.MatchedBy(func(params *ontapRest.ExportPolicyCreateParams) bool {
					return params.Name == "test-policy" &&
						params.SvmName == "testSVM" &&
						len(params.Rules) == 100
				})).Return("new-policy-id", nil)
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockNASClient := new(MockNASClient)
			mockRESTClient := &MockRESTClientForNAS{
				nasClient: mockNASClient,
			}

			getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
				return mockRESTClient, nil
			}

			rc := &OntapRestProvider{Logger: log.NewLogger()}

			tt.setup(mockNASClient)

			err := rc.CreateExportPolicy(tt.policy)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}

			mockNASClient.AssertExpectations(t)
		})
	}
}

// Test for verifying behavior with nil export policy
func TestCreateExportPolicy_NilPolicy(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	// This should cause a panic if we try to access params fields
	defer func() {
		if r := recover(); r != nil {
			t.Logf("Expected panic occurred when passing nil policy: %v", r)
		}
	}()

	// Test with nil policy - this should panic
	err := rc.CreateExportPolicy(nil)
	assert.Error(t, err)
}

// Test to verify all protocol combinations are handled correctly
func TestConvertStorageExportPolicyRuleToONTAP_AllProtocolCombinations(t *testing.T) {
	tests := []struct {
		name     string
		rule     ExportRule
		expected []string
	}{
		{
			name:     "No protocols",
			rule:     ExportRule{CIFS: false, NFSv3: false, NFSv4: false},
			expected: nil,
		},
		{
			name:     "Only CIFS",
			rule:     ExportRule{CIFS: true, NFSv3: false, NFSv4: false},
			expected: []string{utils.GetOntapValue(utils.ProtocolSMB)},
		},
		{
			name:     "Only NFSv3",
			rule:     ExportRule{CIFS: false, NFSv3: true, NFSv4: false},
			expected: []string{utils.GetOntapValue(utils.ProtocolNFSv3)},
		},
		{
			name:     "Only NFSv4",
			rule:     ExportRule{CIFS: false, NFSv3: false, NFSv4: true},
			expected: []string{utils.GetOntapValue(utils.ProtocolNFSv4)},
		},
		{
			name:     "CIFS and NFSv3",
			rule:     ExportRule{CIFS: true, NFSv3: true, NFSv4: false},
			expected: []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv3)},
		},
		{
			name:     "CIFS and NFSv4",
			rule:     ExportRule{CIFS: true, NFSv3: false, NFSv4: true},
			expected: []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv4)},
		},
		{
			name:     "NFSv3 and NFSv4",
			rule:     ExportRule{CIFS: false, NFSv3: true, NFSv4: true},
			expected: []string{utils.GetOntapValue(utils.ProtocolNFSv3), utils.GetOntapValue(utils.ProtocolNFSv4)},
		},
		{
			name:     "All protocols",
			rule:     ExportRule{CIFS: true, NFSv3: true, NFSv4: true},
			expected: []string{utils.GetOntapValue(utils.ProtocolSMB), utils.GetOntapValue(utils.ProtocolNFSv3), utils.GetOntapValue(utils.ProtocolNFSv4)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertStorageExportPolicyRuleToONTAP(tt.rule)
			assert.Equal(t, tt.expected, result.Protocols)
		})
	}
}

// Test to verify authentication rule combinations
func TestConvertStorageExportPolicyRuleToONTAP_AuthenticationRules(t *testing.T) {
	tests := []struct {
		name       string
		rule       ExportRule
		expectedRO string
		expectedRW string
		expectedSU string
	}{
		{
			name:       "No protocols, no superuser",
			rule:       ExportRule{CIFS: false, NFSv3: false, NFSv4: false, Superuser: false},
			expectedRO: models.ExportAuthenticationFlavorNever,
			expectedRW: models.ExportAuthenticationFlavorNever,
			expectedSU: models.NoneAccessProtocol,
		},
		{
			name:       "With protocols, no superuser",
			rule:       ExportRule{CIFS: true, NFSv3: false, NFSv4: false, Superuser: false},
			expectedRO: models.ExportAuthenticationFlavorSys,
			expectedRW: models.AnyAccessProtocol,
			expectedSU: models.NoneAccessProtocol,
		},
		{
			name:       "No protocols, with superuser",
			rule:       ExportRule{CIFS: false, NFSv3: false, NFSv4: false, Superuser: true},
			expectedRO: models.ExportAuthenticationFlavorNever,
			expectedRW: models.ExportAuthenticationFlavorNever,
			expectedSU: models.AnyAccessProtocol,
		},
		{
			name:       "With protocols and superuser",
			rule:       ExportRule{CIFS: true, NFSv3: true, NFSv4: true, Superuser: true},
			expectedRO: models.ExportAuthenticationFlavorSys,
			expectedRW: models.AnyAccessProtocol,
			expectedSU: models.AnyAccessProtocol,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertStorageExportPolicyRuleToONTAP(tt.rule)
			assert.Equal(t, tt.expectedRO, result.ReadOnlyRule)
			assert.Equal(t, tt.expectedRW, result.ReadWriteRule)
			assert.Equal(t, tt.expectedSU, result.SuperUserRule)
		})
	}
}

func TestOntapRestProvider_UpdateExportPolicyRules(t *testing.T) {
	t.Run("WhenGetOntapClientFuncFails_ShouldReturnError", func(t *testing.T) {
		provider := &OntapRestProvider{Logger: log.NewLogger()}

		// Mock getOntapClientFunc to return error
		originalFunc := getOntapClientFunc
		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}
		defer func() { getOntapClientFunc = originalFunc }()

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules:      []*ExportRule{},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get ONTAP client")
	})

	t.Run("WhenExportPolicyGetFails_ShouldReturnError", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "test-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(nil, errors.New("export policy get failed"))

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules:      []*ExportRule{},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get export policy")
	})

	t.Run("WhenExportPolicyDoesNotExist_ShouldSkipUpdate", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		// Mock export policy not found
		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "test-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(nil, nil) // Return nil for both policy and error to indicate policy doesn't exist

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules:      []*ExportRule{},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.NoError(t, err) // Should not error when policy doesn't exist (since export policy rules are optional)
	})

	t.Run("WhenExportPolicyNameIsDifferent_AndCreatePolicyFails_ShouldReturnError", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		volumeResp := &ontapRest.Volume{
			Volume: ontaprestmodels.Volume{
				Nas: &ontaprestmodels.VolumeInlineNas{
					ExportPolicy: &ontaprestmodels.VolumeInlineNasInlineExportPolicy{
						Name: nillable.ToPointer("current-policy"),
					},
				},
			},
		}
		mockStorageClient.On("VolumeGet", mock.MatchedBy(func(params *ontapRest.VolumeGetParams) bool {
			return params.Name == "test-volume" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(volumeResp, nil)

		// Mock target policy doesn't exist
		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "new-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(nil, errors.New("policy not found"))

		// Mock create policy fails
		mockNASClient.On("ExportPolicyCreate", mock.Anything).
			Return("", errors.New("create failed"))

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "new-policy",
				ExportRules: []*ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						NFSv3:          true,
					},
				},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get export policy")
	})

	t.Run("WhenExportPolicyNameIsSame_AndCurrentPolicyGetFails_ShouldReturnError", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		volumeResp := &ontapRest.Volume{
			Volume: ontaprestmodels.Volume{
				Nas: &ontaprestmodels.VolumeInlineNas{
					ExportPolicy: &ontaprestmodels.VolumeInlineNasInlineExportPolicy{
						Name: nillable.ToPointer("same-policy"),
					},
				},
			},
		}
		mockStorageClient.On("VolumeGet", mock.MatchedBy(func(params *ontapRest.VolumeGetParams) bool {
			return params.Name == "test-volume" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(volumeResp, nil)

		// Mock current policy get fails
		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "same-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(nil, errors.New("get policy failed"))

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "same-policy",
				ExportRules: []*ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						NFSv3:          true,
					},
				},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get export policy")
	})

	t.Run("WhenExportPolicyNameIsSame_AndPolicyModifyFails_ShouldReturnError", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		volumeResp := &ontapRest.Volume{
			Volume: ontaprestmodels.Volume{
				Nas: &ontaprestmodels.VolumeInlineNas{
					ExportPolicy: &ontaprestmodels.VolumeInlineNasInlineExportPolicy{
						Name: nillable.ToPointer("same-policy"),
					},
				},
			},
		}
		mockStorageClient.On("VolumeGet", mock.MatchedBy(func(params *ontapRest.VolumeGetParams) bool {
			return params.Name == "test-volume" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(volumeResp, nil)

		// Mock current policy get
		currentPolicy := &ontapRest.ExportPolicy{
			ExportPolicy: ontaprestmodels.ExportPolicy{
				ID: nillable.ToPointer(int64(123)),
			},
		}
		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "same-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(currentPolicy, nil)

		// Mock policy modify fails
		mockNASClient.On("ExportPolicyModify", mock.Anything).
			Return(errors.New("modify failed"))

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "same-policy",
				ExportRules: []*ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						NFSv3:          true,
					},
				},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update export policy")
	})

	t.Run("WhenSuccess_ShouldUpdatePolicyRules", func(t *testing.T) {
		mockNASClient := new(MockNASClient)
		mockStorageClient := new(ontapRest.MockStorageClient)
		mockRESTClient := &MockRESTClientForNAS{
			nasClient:     mockNASClient,
			storageClient: mockStorageClient,
		}

		getOntapClientFunc = func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockRESTClient, nil
		}
		defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

		volumeResp := &ontapRest.Volume{
			Volume: ontaprestmodels.Volume{
				Nas: &ontaprestmodels.VolumeInlineNas{
					ExportPolicy: &ontaprestmodels.VolumeInlineNasInlineExportPolicy{
						Name: nillable.ToPointer("test-policy"),
					},
				},
			},
		}
		mockStorageClient.On("VolumeGet", mock.MatchedBy(func(params *ontapRest.VolumeGetParams) bool {
			return params.Name == "test-volume" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(volumeResp, nil)

		// Mock current policy get
		currentPolicy := &ontapRest.ExportPolicy{
			ExportPolicy: ontaprestmodels.ExportPolicy{
				ID: nillable.ToPointer(int64(123)),
			},
		}
		mockNASClient.On("ExportPolicyGet", mock.MatchedBy(func(params *ontapRest.ExportPolicyGetParams) bool {
			return params.Name != nil && *params.Name == "test-policy" && params.SvmName != nil && *params.SvmName == "test-svm"
		})).Return(currentPolicy, nil)

		// Mock policy modify success
		mockNASClient.On("ExportPolicyModify", mock.Anything).
			Return(nil)

		provider := &OntapRestProvider{Logger: log.NewLogger()}

		params := UpdateExportPolicyRulesParams{
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			ExportPolicy: &ExportPolicy{
				ExportPolicyName: "test-policy",
				ExportRules: []*ExportRule{
					{
						AllowedClients: "192.168.1.0/24",
						NFSv3:          true,
						Superuser:      true,
					},
				},
			},
		}

		err := provider.UpdateExportPolicyRules(params)
		assert.NoError(t, err)

		mockNASClient.AssertCalled(t, "ExportPolicyModify", mock.Anything)
	})
}

var originalGetOntapClientFunc = getOntapClientFunc

func TestDeleteExportPolicy_Success(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
	}

	mockNASClient.On("ExportPolicyDelete", mock.MatchedBy(func(params *ontapRest.ExportPolicyDeleteParams) bool {
		return params.Name == exportPolicy.ExportPolicyName && params.SvmName == exportPolicy.SvmName
	})).Return(nil)

	err := rc.DeleteExportPolicy(exportPolicy)

	assert.NoError(t, err)
	mockNASClient.AssertExpectations(t)
}

func TestDeleteExportPolicy_GetClientFailure(t *testing.T) {
	originalGetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalGetOntapClientFunc }()

	expectedError := errors.New("failed to get client")
	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, expectedError
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
	}

	err := rc.DeleteExportPolicy(exportPolicy)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP client")
	assert.Contains(t, err.Error(), expectedError.Error())
}

func TestDeleteExportPolicy_DeleteFailure(t *testing.T) {
	mockNASClient := new(MockNASClient)
	mockRESTClient := &MockRESTClientForNAS{
		nasClient: mockNASClient,
	}

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockRESTClient, nil
	}

	rc := &OntapRestProvider{Logger: log.NewLogger()}

	exportPolicy := &ExportPolicy{
		ExportPolicyName: "test-policy",
		SvmName:          "testSVM",
	}

	expectedError := errors.New("delete failed")
	mockNASClient.On("ExportPolicyDelete", mock.MatchedBy(func(params *ontapRest.ExportPolicyDeleteParams) bool {
		return params.Name == exportPolicy.ExportPolicyName && params.SvmName == exportPolicy.SvmName
	})).Return(expectedError)

	err := rc.DeleteExportPolicy(exportPolicy)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete export policy test-policy")
	assert.Contains(t, err.Error(), expectedError.Error())
	mockNASClient.AssertExpectations(t)
}
