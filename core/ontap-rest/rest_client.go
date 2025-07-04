package ontap_rest

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/go-openapi/runtime"
	rtclient "github.com/go-openapi/runtime/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	clientPriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client"
	ottransport "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// RESTClient describes an ONTAP REST client
type RESTClient interface { // generate:mock
	Host() string
	Cluster() ClusterClient
	Cloud() CloudClient
	SAN() SANClient
	Networking() NetworkingClient
	Security() SecurityClient
	Storage() StorageClient
	SVM() SVMClient
	Poll(jobUUID string) error
	Snapmirror() SnapmirrorClient
}

type OntapRestClient struct {
	params                    RESTClientParams
	httpRoundTripperTransport *http.Transport
	cluster                   *clusterClient
	san                       *sanClient
	security                  *securityClient
	networking                *networkingClient
	storage                   *storageClient
	svm                       *svmClient
	poller                    Poller
	snapmirror                *snapmirrorClient
	cloud                     *cloudClient
}

// RESTClientParams describes the parameters for creating a new RESTClient
type RESTClientParams struct {
	Hosts              []string
	Host               string
	Username           string
	Password           log.Secret
	InsecureSkipVerify bool
	Trace              *log.Slogger
}

var (
	ontapRestLogVerbose = env.GetBool("ONTAP_REST_LOG_VERBOSE", false)
	TestConnection      = testConnection // Allow overriding for testing purposes
)

var (
	NewOntapRestClient = NewClient
)

func NewClient(params RESTClientParams) (RESTClient, error) {
	useCert := false

	var lastErr error
	var rClient *OntapRestClient
	for _, host := range params.Hosts {
		tryParams := params
		// Set Host for this attempt
		tryParams.Host = host

		rt := newRuntimeClient(tryParams)
		httpRoundTripperTransport := &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          100,
			IdleConnTimeout:       time.Second * 90,
			TLSHandshakeTimeout:   time.Second * 10,
			ExpectContinueTimeout: time.Second,
			TLSClientConfig: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: tryParams.InsecureSkipVerify,
			},
		}

		var rc *OntapRestClient
		rt.Transport = httpRoundTripperTransport
		rt.Transport = ottransport.NewLoggingRoundTripper(tryParams.Trace, ontapRestLogVerbose, useCert, rt.Transport)
		rt.Transport = ottransport.NewPaginationRoundTripper(rt.Transport)
		rt.Transport = ottransport.NewAuthenticationRoundTripper(rt.Transport, tryParams.Username, tryParams.Password, useCert)
		retryTransport := ottransport.NewRetryTransport(tryParams.Trace, rt)
		idempotentTransport := ottransport.NewIdempotentTransport(retryTransport, func(operation *runtime.ClientOperation) (interface{}, error) {
			return resolveRESTClientRouterConflict(*tryParams.Trace, rc, operation)
		})
		api := client.New(idempotentTransport, nil)
		apiPriv := clientPriv.New(idempotentTransport, nil)
		p := &poller{api: api.Cluster, logger: tryParams.Trace}
		rc = &OntapRestClient{
			httpRoundTripperTransport: httpRoundTripperTransport,
			params:                    tryParams,
			cluster:                   &clusterClient{api: api.Cluster, apiPriv: &apiPriv.Operations},
			cloud:                     &cloudClient{api: api.Cloud},
			svm:                       &svmClient{api: api.Svm, apiPriv: &apiPriv.Operations},
			networking:                &networkingClient{api: api.Networking, apiPriv: &apiPriv.Operations},
			storage:                   &storageClient{api: api.Storage},
			san:                       &sanClient{api: api.San},
			snapmirror:                &snapmirrorClient{api: api.Snapmirror, apiPriv: apiPriv.Snapmirror},
			poller:                    p,
			security:                  &securityClient{api: &api.Security},
		}
		if err := TestConnection(rc); err == nil {
			return rc, nil
		} else {
			lastErr = err
			rClient = rc
			continue
		}
	}
	if lastErr != nil {
		params.Trace.Errorf("Failed to connect to any ONTAP REST API host: %v", lastErr)
	}
	params.Trace.Warnf("returning client with last tried host")
	return rClient, lastErr
}

// testConnection tries a simple API call to verify connectivity
func testConnection(rc *OntapRestClient) error {
	// Try to get cluster info (adjust as needed for your API)
	if rc.cluster == nil || rc.cluster.api == nil {
		return fmt.Errorf("cluster client not initialized")
	}
	_, err := rc.cluster.api.ClusterGet(cluster.NewClusterGetParams().WithFields([]string{"version"}), nil)
	return err
}

func newRuntimeClient(params RESTClientParams) *rtclient.Runtime {
	rt := rtclient.New(params.Host, client.DefaultBasePath, client.DefaultSchemes)
	rt.Producers["application/hal+json"] = runtime.JSONProducer()
	rt.Consumers["application/hal+json"] = runtime.JSONConsumer()
	rt.Consumers["text/html"] = runtime.JSONConsumer()
	return rt
}

// Host returns the hostname of the REST API
func (rc *OntapRestClient) Host() string {
	return rc.params.Host
}

// Cluster returns a cluster client
func (rc *OntapRestClient) Cluster() ClusterClient {
	return rc.cluster
}

// Networking returns a networking client
func (rc *OntapRestClient) Networking() NetworkingClient {
	return rc.networking
}

// Storage returns a storage client
func (rc *OntapRestClient) Storage() StorageClient {
	return rc.storage
}

// SVM returns an SVM client
func (rc *OntapRestClient) SVM() SVMClient {
	return rc.svm
}

// Poll polls the job with the given UUID
func (rc *OntapRestClient) Poll(jobUUID string) error {
	return rc.poller.Poll(jobUUID)
}

// Security returns a security client
func (rc *OntapRestClient) Security() SecurityClient {
	return rc.security
}

// SAN returns a SAN client
func (rc *OntapRestClient) SAN() SANClient {
	return rc.san
}

// Snapmirror returns a Snapmirror client
func (rc *OntapRestClient) Snapmirror() SnapmirrorClient {
	return rc.snapmirror
}

// Cloud returns a Cloud client
func (rc *OntapRestClient) Cloud() CloudClient {
	return rc.cloud
}
