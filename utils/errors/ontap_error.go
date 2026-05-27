package errors

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-openapi/runtime"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func ParseOntapError(ctx context.Context, err error) error {
	log := util.GetLogger(ctx)
	if err == nil {
		return nil
	}
	if apiError, ok := err.(*runtime.APIError); ok {
		log.Infof("Parsing ONTAP error: %s", apiError.Error())
		if apiError.Code == 202 || apiError.Code == 201 {
			return nil
		}
	}
	s, errMarshal := json.MarshalIndent(err, "", "\t")
	if errMarshal != nil {
		log.Errorf("Failed to marshal error: %v", errMarshal)
		return err
	}
	return errors.New(string(s))
}
