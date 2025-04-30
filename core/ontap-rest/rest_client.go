package ontap_rest

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/go-openapi/runtime"
	rtclient "github.com/go-openapi/runtime/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client"
	clientPriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client"
	ottransport "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// RESTClient describes an ONTAP REST client
type RESTClient interface { // generate:mock
	Host() string
	Cluster() ClusterClient
	SAN() SANClient
	Networking() NetworkingClient
	Security() SecurityClient
	Storage() StorageClient
	SVM() SVMClient
	Poll(jobUUID string) error
}

type restClient struct {
	params                    RESTClientParams
	httpRoundTripperTransport *http.Transport
	cluster                   *clusterClient
	san                       *sanClient
	security                  *securityClient
	networking                *networkingClient
	storage                   *storageClient
	svm                       *svmClient
	poller                    Poller
}

// RESTClientParams describes the parameters for creating a new RESTClient
type RESTClientParams struct {
	Host               string
	Username           string
	Password           log.Secret
	InsecureSkipVerify bool
	Trace              log.Logger
}

var (
	ontapRestLogVerbose = env.GetBool("ONTAP_REST_LOG_VERBOSE", false)
)

var NewOntapRestClient = NewClient

func NewClient(params RESTClientParams) RESTClient {
	useCert := false

	rt := newRuntimeClient(params)
	// MD: We need to cache this object since it keeps the connection pool that is to be re-used when tunneling
	// MD: These values were fetched from the swagger default http Transport
	// it is better to have the values written down instead of them being imported
	// lest they might change during an update and mess things up for us.
	httpRoundTripperTransport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		IdleConnTimeout:       time.Second * 90,
		TLSHandshakeTimeout:   time.Second * 10,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: params.InsecureSkipVerify,
			// Certificates:       certs,
		},
	}

	var rc *restClient
	rt.Transport = httpRoundTripperTransport
	// rt.Transport = tracing.NewTracingTransport(rt.Transport)
	rt.Transport = ottransport.NewLoggingRoundTripper(params.Trace, ontapRestLogVerbose, useCert, rt.Transport)
	rt.Transport = ottransport.NewPaginationRoundTripper(rt.Transport)
	rt.Transport = ottransport.NewAuthenticationRoundTripper(rt.Transport, params.Username, params.Password, useCert)
	retryTransport := ottransport.NewRetryTransport(params.Trace, rt)
	idempotentTransport := ottransport.NewIdempotentTransport(retryTransport, func(operation *runtime.ClientOperation) (interface{}, error) {
		return resolveRESTClientRouterConflict(params.Trace, rc, operation)
	})
	api := client.New(idempotentTransport, nil)
	apiPriv := clientPriv.New(idempotentTransport, nil)
	p := &poller{api: api.Cluster, logger: params.Trace}
	rc = &restClient{
		httpRoundTripperTransport: httpRoundTripperTransport,
		params:                    params,
		cluster:                   &clusterClient{api: api.Cluster},
		svm:                       &svmClient{api: api.Svm, apiPriv: &apiPriv.Operations},
		networking:                &networkingClient{api: api.Networking, apiPriv: &apiPriv.Operations},
		storage:                   &storageClient{api: api.Storage},
		san:                       &sanClient{api: api.San},
		poller:                    p,
	}
	return rc
}

func newRuntimeClient(params RESTClientParams) *rtclient.Runtime {
	rt := rtclient.New(params.Host, client.DefaultBasePath, client.DefaultSchemes)
	rt.Producers["application/hal+json"] = runtime.JSONProducer()
	rt.Consumers["application/hal+json"] = runtime.JSONConsumer()
	rt.Consumers["text/html"] = runtime.JSONConsumer()
	return rt
}

// Host returns the hostname of the REST API
func (rc *restClient) Host() string {
	return rc.params.Host
}

// Cluster returns a cluster client
func (rc *restClient) Cluster() ClusterClient {
	return rc.cluster
}

// Networking returns a networking client
func (rc *restClient) Networking() NetworkingClient {
	return rc.networking
}

// Storage returns a storage client
func (rc *restClient) Storage() StorageClient {
	return rc.storage
}

// SVM returns an SVM client
func (rc *restClient) SVM() SVMClient {
	return rc.svm
}

// Poll polls the job with the given UUID
func (rc *restClient) Poll(jobUUID string) error {
	return rc.poller.Poll(jobUUID)
}

// Security returns a security client
func (rc *restClient) Security() SecurityClient {
	return rc.security
}

// SAN returns a SAN client
func (rc *restClient) SAN() SANClient {
	return rc.san
}
