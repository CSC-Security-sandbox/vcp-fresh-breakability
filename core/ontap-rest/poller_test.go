package ontap_rest

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestPoll(t *testing.T) {
	uuid := "job-uuid"

	t.Run("WhenJobGetFails_ThenReturnError", func(tt *testing.T) {
		mcs := cluster.NewMockClientService(t)
		pp := &poller{api: mcs}

		expectedErr := errors.New("expected error")
		go func() {
			defer mcs.MockClientServiceDone()

			err := pp.Poll(uuid)
			assert.Equal(tt, expectedErr, err)
		}()
		mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, nil, expectedErr)
		mcs.AssertMockClientServiceDone()
	})

	t.Run("WhenJobStateIsFailure_ThenReturnErrorMessage", func(tt *testing.T) {
		mcs := cluster.NewMockClientService(t)
		pp := &poller{api: mcs}

		rsp := &cluster.JobGetOK{Payload: &models.Job{
			State: nillable.ToPointer("failure"),
			Error: &models.JobInlineError{
				Code:    nillable.ToPointer("5812345"),
				Message: nillable.ToPointer("Don't call me"),
			},
		}}
		go func() {
			defer mcs.MockClientServiceDone()

			err := pp.Poll(uuid)
			assert.Equal(tt, "Don't call me", err.Error())
		}()
		mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, rsp, nil)
		mcs.AssertMockClientServiceDone()
	})

	t.Run("WhenPollingTimesOut_ThenReturnTimeoutError", func(tt *testing.T) {
		mcs := cluster.NewMockClientService(t)
		pp := &poller{logger: *log.NewLogger().(*log.Slogger), api: mcs}

		oldTimeout := timeout
		oldWait := wait
		defer func() {
			timeout = oldTimeout
			wait = oldWait
		}()
		timeout = time.Millisecond
		wait = time.Millisecond * 10

		go func() {
			defer mcs.MockClientServiceDone()

			err := pp.Poll(uuid)
			assert.EqualError(tt, err, "polling for ontap-rest job with UUID '"+uuid+"' timed out")
		}()

		mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
			State: nillable.ToPointer("not ready"),
		}}, nil)
		mcs.AssertMockClientServiceDone()
	})

	t.Run("WhenJobStateIsSuccess_ThenReturnNoError", func(tt *testing.T) {
		mcs := cluster.NewMockClientService(t)
		pp := &poller{api: mcs}

		go func() {
			defer mcs.MockClientServiceDone()

			err := pp.Poll(uuid)
			assert.Nil(tt, err)
		}()
		mcs.AssertJobGet(cluster.NewJobGetParams().WithUUID(uuid).WithFields([]string{"*", "node.name"}), nil, nil, &cluster.JobGetOK{Payload: &models.Job{
			State: nillable.ToPointer("success"),
		}}, nil)
		mcs.AssertMockClientServiceDone()
	})
}
