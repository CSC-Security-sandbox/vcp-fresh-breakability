package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type IAction interface {
	ShouldAllow(r *http.Request) bool
	ProcessRequest(r *http.Request, w http.ResponseWriter) error
	ProcessResponse(resp *http.Response) error
}

type Allow struct {
	Name         string
	RemoveFields []string
	AddHeaders   map[string]string
}

func (a Allow) ShouldAllow(r *http.Request) bool {
	return true
}

func (a Allow) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	logger := log.NewLogger()
	logger.Info("Processing request", "action", a.Name)
	return nil
}

func (a Allow) ProcessResponse(resp *http.Response) error {
	logger := log.NewLogger()
	logger.Info("Processing response", "action", a.Name)
	return a.applyModifications(resp)
}

func (a Allow) applyModifications(resp *http.Response) error {
	if a.AddHeaders != nil {
		for key, value := range a.AddHeaders {
			resp.Header.Set(key, value)
		}
	}

	if len(a.RemoveFields) > 0 {
		if err := a.removeJSONFields(resp); err != nil {
			logger := log.NewLogger()
			logger.Error("Error removing fields", "error", err)
			return err
		}
	}

	return nil
}

func (a Allow) removeJSONFields(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var data interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("response is not valid JSON, cannot remove fields: %v", err)
	}

	a.removeFields(data)

	newBody, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp.Body = io.NopCloser(bytes.NewReader(newBody))
	resp.ContentLength = int64(len(newBody))

	logger := log.NewLogger()
	logger.Info("Removed fields", "fields", a.RemoveFields)
	return nil
}

func (a Allow) removeFields(data interface{}) {
	switch v := data.(type) {
	case map[string]interface{}:
		for _, field := range a.RemoveFields {
			if _, exists := v[field]; exists {
				logger := log.NewLogger()
				logger.Info("Removing field", "field", field)
				delete(v, field)
			}
		}

		for _, value := range v {
			a.removeFields(value)
		}

	case []interface{}:
		for _, item := range v {
			a.removeFields(item)
		}
	}
}

type Deny struct {
	Name string
}

func (d Deny) ShouldAllow(r *http.Request) bool {
	return false
}

func (d Deny) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	logger := log.NewLogger()
	logger.Info("Processing deny", "action", d.Name)
	http.Error(w, "Forbidden", http.StatusForbidden)
	return nil
}

func (d Deny) ProcessResponse(resp *http.Response) error {
	return nil
}

func DenyAll() IAction {
	return Deny{Name: "Access denied"}
}
