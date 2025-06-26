package common

import (
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

var executeActivity = workflow.ExecuteActivity

type rollbackActivity struct {
	activity interface{}
	args     []interface{}
}

type RollbackManager struct {
	rollbacks []rollbackActivity
}

func NewRollbackManager() *RollbackManager {
	return &RollbackManager{
		rollbacks: []rollbackActivity{},
	}
}

// Add stores the rollback activity and its arguments
func (rm *RollbackManager) Add(activity interface{}, args ...interface{}) {
	rm.rollbacks = append(rm.rollbacks, rollbackActivity{
		activity: activity,
		args:     args,
	})
}

// ExecuteRollback performs compensations in LIFO order
func (rm *RollbackManager) ExecuteRollback(ctx workflow.Context, err error) {
	// Sequential rollback in LIFO order
	logger := util.GetLogger(ctx)

	for i := len(rm.rollbacks) - 1; i >= 0; i-- {
		r := rm.rollbacks[i]

		errorMessage := vsaerrors.ExtractCustomerFacingErrorMessage(ctx, err)
		r.args = append(r.args, errorMessage)
		fut := executeActivity(ctx, r.activity, r.args...)
		if errComp := fut.Get(ctx, nil); errComp != nil {
			err = errComp
			logger.Errorf("Error executing rollback, err: %v", err)
		}
	}
}
