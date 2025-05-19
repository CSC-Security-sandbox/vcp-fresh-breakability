package ontap_rest

import (
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/go-openapi/runtime"
	rtclient "github.com/go-openapi/runtime/client"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type mockTransport struct {
	response interface{}
	err      error
}

func (mock *mockTransport) Submit(*runtime.ClientOperation) (interface{}, error) {
	return mock.response, mock.err
}

func TestNewClient(t *testing.T) {
	params := RESTClientParams{
		Host:               "localhost",
		Username:           "admin",
		Password:           log.Secret("secret"),
		InsecureSkipVerify: true,
		Trace:              log.NewLogger().(*log.Slogger),
	}

	f := func(rcl RESTClient) {
		if c, ok := rcl.(*restClient); !ok {
			t.Error("Client type does not match expected one")
		} else {
			assert.NotNil(t, c.httpRoundTripperTransport.Proxy)
			assert.Equal(t, 100, c.httpRoundTripperTransport.MaxIdleConns)
			assert.Equal(t, time.Second*90, c.httpRoundTripperTransport.IdleConnTimeout)
			assert.Equal(t, time.Second*10, c.httpRoundTripperTransport.TLSHandshakeTimeout)
			assert.Equal(t, time.Second*1, c.httpRoundTripperTransport.ExpectContinueTimeout)
			assert.Equal(t, params, c.params)

			assert.NotNil(t, c.cluster.api)
			assert.NotNil(t, c.networking.api)
			assert.NotNil(t, (*c.networking.apiPriv).(*operations.Client))

			fv := reflect.ValueOf(c.cluster.api).Elem().FieldByName("transport")
			it := reflect.NewAt(fv.Type(), unsafe.Pointer(fv.UnsafeAddr())).Elem().Interface().(*transport.IdempotentTransport)

			fv = reflect.ValueOf(it).Elem().FieldByName("transport")
			rt := reflect.NewAt(fv.Type(), unsafe.Pointer(fv.UnsafeAddr())).Elem().Interface().(*transport.RetryTransport)

			fv = reflect.ValueOf(rt).Elem().FieldByName("transport")
			rtc := reflect.NewAt(fv.Type(), unsafe.Pointer(fv.UnsafeAddr())).Elem().Interface().(*rtclient.Runtime)
			assert.NotNil(t, rtc.Producers["application/hal+json"])
			assert.NotNil(t, rtc.Consumers["application/hal+json"])
			assert.NotNil(t, rtc.Consumers["text/html"])

			_, ok := rtc.Transport.(*transport.AuthenticationRoundTripper)
			if !ok {
				t.Errorf("Expected *transport.AuthenticationRoundTripper but got: %v", rtc.Transport)
			}
		}
	}

	cl := NewClient(params)
	f(cl)
}

func TestRESTClient_Host(t *testing.T) {
	client := &restClient{params: RESTClientParams{Host: "host"}}
	assert.Equal(t, "host", client.Host())
}

func TestRESTClient_Cluster(t *testing.T) {
	cc := &clusterClient{}
	client := &restClient{cluster: cc}
	assert.Equal(t, cc, client.Cluster())
}

func TestRESTClient_Networking(t *testing.T) {
	nc := &networkingClient{}
	client := &restClient{networking: nc}
	assert.Equal(t, nc, client.Networking())
}

func TestRESTClient_Security(t *testing.T) {
	sc := &securityClient{}
	client := &restClient{security: sc}
	assert.Equal(t, sc, client.Security())
}

func TestRESTClient_Storage(t *testing.T) {
	sc := &storageClient{}
	client := &restClient{storage: sc}
	assert.Equal(t, sc, client.Storage())
}

func TestRESTClient_SVM(t *testing.T) {
	svmc := &svmClient{}
	client := &restClient{svm: svmc}
	assert.Equal(t, svmc, client.SVM())
}

func TestRESTClient_SAN(t *testing.T) {
	sanc := &sanClient{}
	client := &restClient{san: sanc}
	assert.Equal(t, sanc, client.SAN())
}

func TestRESTClient_Poll(t *testing.T) {
	mockPoller := new(MockPoller)
	client := &restClient{poller: mockPoller}
	mockPoller.On("Poll", "job-uuid").Return(nil)

	err := client.Poll("job-uuid")
	assert.NoError(t, err)
	mockPoller.AssertCalled(t, "Poll", "job-uuid")
}
