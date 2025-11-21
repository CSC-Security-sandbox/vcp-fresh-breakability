package overridemodels

import (
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
)

func TestSetClientRequestWriterForLdapPreferredAdServer(t *testing.T) {
	testIP := "0.0.0.0"
	lsm := LdapServiceModified{}
	t.Run("ClientRequestWritersShouldReturnErrorWhenSetBodyOrPathParamFails", func(t *testing.T) {
		params := &name_services.LdapModifyParams{
			Info: &models.LdapService{
				LdapServiceInlinePreferredAdServers: []*string{&testIP},
			},
			SvmUUID: "svm-uuid",
		}

		clientOpt, err := lsm.SetClientRequestWriterForLdapPreferredAdServer(params)
		assert.NoError(t, err)
		clientOpt(&runtime.ClientOperation{
			Params: runtime.ClientRequestWriterFunc(func(req runtime.ClientRequest, _ strfmt.Registry) error {
				return assert.AnError
			}),
		})
	})
	t.Run("ClientRequestWritersShouldSetBodyParams", func(t *testing.T) {
		params := &name_services.LdapModifyParams{
			Info: &models.LdapService{
				LdapServiceInlinePreferredAdServers: []*string{&testIP},
			},
			SvmUUID: "svm-uuid",
		}

		clientOpt, err := lsm.SetClientRequestWriterForLdapPreferredAdServer(params)
		assert.NoError(t, err)
		clientOpt(&runtime.ClientOperation{
			Params: runtime.ClientRequestWriterFunc(func(req runtime.ClientRequest, _ strfmt.Registry) error {
				assert.Equal(t, lsm, req.GetBodyParam())
				return nil
			}),
		})
	})
	t.Run("ClientRequestWritersShouldSetPathParam", func(t *testing.T) {
		params := &name_services.LdapModifyParams{
			Info: &models.LdapService{
				LdapServiceInlinePreferredAdServers: []*string{&testIP},
			},
			SvmUUID: "svm-uuid",
		}

		clientOpt, err := lsm.SetClientRequestWriterForLdapPreferredAdServer(params)
		assert.NoError(t, err)
		clientOpt(&runtime.ClientOperation{
			Params: runtime.ClientRequestWriterFunc(func(req runtime.ClientRequest, _ strfmt.Registry) error {
				path := req.GetPath()
				assert.Contains(t, params.SvmUUID, path)
				assert.NotContains(t, "svm.uuid", path)
				return nil
			}),
		})
	})
	t.Run("WhenSuccessful", func(t *testing.T) {
		params := &name_services.LdapModifyParams{
			Info: &models.LdapService{
				LdapServiceInlinePreferredAdServers: []*string{&testIP},
			},
			SvmUUID: "svm-uuid",
		}

		clientOpt, err := lsm.SetClientRequestWriterForLdapPreferredAdServer(params)
		assert.NoError(t, err)
		assert.NotNil(t, clientOpt)
	})
}
