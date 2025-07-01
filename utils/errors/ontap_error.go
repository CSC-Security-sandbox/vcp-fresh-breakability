package errors

import (
	"encoding/json"
	"errors"
	"github.com/go-openapi/runtime"
	"netapp.com/vsa/lifecycle-manager/pkg/log"
)

func ParseOntapError(err error) error {
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
