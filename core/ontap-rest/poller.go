package ontap_rest

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	timeout = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_TIMEOUT_MINUTES", 5)) * time.Minute
	wait    = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
)

// Poller describes a poller that polls a job
type Poller interface { // generate:mock
	Poll(UUID string) error
}

type poller struct {
	api    cluster.ClientService
	logger log.Slogger
}

// Poll polls an ontap job given UUID
func (p *poller) Poll(UUID string) error {
	// MD: all job related logging happens on the transport layer.
	// There is no need to log anything here
	params := cluster.NewJobGetParams().WithUUID(UUID).WithFields([]string{"*", "node.name"})

	t2 := time.Now().Add(timeout)
	for time.Now().Before(t2) {
		rsp, err := p.api.JobGet(params, nil)
		if err != nil {
			return err
		}

		if *rsp.Payload.State == models.JobStateFailure {
			return transport.ConvertFromRESTError(p.logger, rsp)
		}

		if *rsp.Payload.State == models.JobStateSuccess {
			return nil
		}

		time.Sleep(wait)
	}

	p.logger.WithFields(log.Fields{
		"ontap-rest job uuid": UUID,
		"err":                 "job polling timeout",
	}).Error("ontap-rest error")
	return errors.NewTimeoutErr("polling for ontap-rest job with UUID '" + UUID + "' timed out")
}
