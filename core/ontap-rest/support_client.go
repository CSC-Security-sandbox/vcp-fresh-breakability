package ontap_rest

import (
	"strconv"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/support"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// SupportClient describes a support client for EMS operations
type SupportClient interface { // generate:mock
	EMSEventDestinationCreate(params *EMSEventDestinationCreateParams) error
	EMSEventDestinationGet(name string) (*EMSEventDestination, error)
	EMSEventDestinationModify(name string, params *EMSEventDestinationModifyParams) error
	EMSEventDestinationDelete(name string) error
	EMSEventFilterCreate(params *EMSEventFilterCreateParams) error
	EMSEventFilterGet(name string) (*EMSEventFilter, error)
	EMSEventFilterDelete(name string) error
	EMSEventFilterRuleAdd(params *EMSEventFilterRuleAddParams) error
	EMSEventFilterRulesGet(filterName string) ([]*EMSEventFilterRule, error)
	EMSEventFilterRuleDelete(filterName string, index int) error
}

type supportClient struct {
	api *support.ClientService
}

// isAlreadyExistsError checks if an error represents an "already exists" condition.
// It checks for:
// 1. HTTP 409 (Conflict) status code from runtime.APIError
// 2. Error message containing "already exists" (case-insensitive)
// This is more precise than checking for "983" which matches many different ONTAP error codes.
func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	// Check for HTTP 409 (Conflict) status code
	if apiError, ok := err.(*runtime.APIError); ok {
		if apiError.Code == 409 {
			return true
		}
	}

	// Check for "already exists" in error message (case-insensitive)
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "already exists")
}

// isAlreadyLinkedError checks if an error represents an "already linked" condition.
// It checks for:
// 1. HTTP 409 (Conflict) status code from runtime.APIError
// 2. Error message containing "already" (case-insensitive)
func isAlreadyLinkedError(err error) bool {
	if err == nil {
		return false
	}

	// Check for HTTP 409 (Conflict) status code
	if apiError, ok := err.(*runtime.APIError); ok {
		if apiError.Code == 409 {
			return true
		}
	}

	// Check for "already" in error message (case-insensitive)
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "already")
}

// EMSEventDestinationCreate creates an EMS event notification destination
func (sc *supportClient) EMSEventDestinationCreate(params *EMSEventDestinationCreateParams) error {
	if params == nil || params.Name == nil {
		return errors.New("destination name is required")
	}

	otParams := support.NewEmsDestinationCreateParams()
	otParams.SetInfo(emsDestinationCreateParamsToONTAP(params))

	response, err := (*sc.api).EmsDestinationCreate(otParams, nil)
	if err != nil {
		// Check if destination already exists (idempotent)
		if isAlreadyExistsError(err) {
			return nil // Ignore "already exists" errors
		}
		return err
	}

	// Check if response indicates success
	if response == nil {
		return errors.New("unexpected response from EmsDestinationCreate")
	}

	return nil
}

// EMSEventDestinationGet gets an EMS event notification destination
func (sc *supportClient) EMSEventDestinationGet(name string) (*EMSEventDestination, error) {
	if name == "" {
		return nil, errors.New("destination name is required")
	}

	otParams := support.NewEmsDestinationGetParams()
	otParams.SetName(name)

	response, err := (*sc.api).EmsDestinationGet(otParams, nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from EmsDestinationGet")
	}

	return emsDestinationFromONTAP(response.Payload), nil
}

// EMSEventDestinationModify modifies an EMS event destination to link filters
func (sc *supportClient) EMSEventDestinationModify(name string, params *EMSEventDestinationModifyParams) error {
	if name == "" {
		return errors.New("destination name is required")
	}

	otParams := support.NewEmsDestinationModifyParams()
	otParams.SetName(name)
	otParams.SetInfo(emsDestinationModifyParamsToONTAP(params))

	response, err := (*sc.api).EmsDestinationModify(otParams, nil)
	if err != nil {
		// Check if already linked (idempotent)
		if isAlreadyLinkedError(err) {
			return nil // Ignore "already linked" errors
		}
		return err
	}

	if response == nil {
		return errors.New("unexpected response from EmsDestinationModify")
	}

	return nil
}

// EMSEventFilterCreate creates an EMS event filter
func (sc *supportClient) EMSEventFilterCreate(params *EMSEventFilterCreateParams) error {
	if params == nil || params.Name == nil {
		return errors.New("filter name is required")
	}

	otParams := support.NewEmsFilterCreateParams()
	otParams.SetInfo(emsFilterCreateParamsToONTAP(params))

	response, err := (*sc.api).EmsFilterCreate(otParams, nil)
	if err != nil {
		// Check if filter already exists (idempotent)
		if isAlreadyExistsError(err) {
			return nil // Ignore "already exists" errors
		}
		return err
	}

	if response == nil {
		return errors.New("unexpected response from EmsFilterCreate")
	}

	return nil
}

// EMSEventFilterGet gets an EMS event filter
func (sc *supportClient) EMSEventFilterGet(name string) (*EMSEventFilter, error) {
	if name == "" {
		return nil, errors.New("filter name is required")
	}

	otParams := support.NewEmsFilterGetParams()
	otParams.SetName(name)

	response, err := (*sc.api).EmsFilterGet(otParams, nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from EmsFilterGet")
	}

	return emsFilterFromONTAP(response.Payload), nil
}

// EMSEventFilterDelete deletes an EMS event filter
func (sc *supportClient) EMSEventFilterDelete(name string) error {
	if name == "" {
		return errors.New("filter name is required")
	}

	otParams := support.NewEmsFilterDeleteParams()
	otParams.SetName(name)
	_, err := (*sc.api).EmsFilterDelete(otParams, nil)
	if err != nil {
		// Check if filter doesn't exist (idempotent)
		if errors.IsNotFoundErr(err) {
			return nil // Ignore "not found" errors
		}
		return err
	}
	return nil
}

// EMSEventFilterRuleAdd adds a rule to an EMS event filter
func (sc *supportClient) EMSEventFilterRuleAdd(params *EMSEventFilterRuleAddParams) error {
	if params == nil || params.FilterName == nil {
		return errors.New("filter name is required")
	}

	otParams := support.NewEmsFiltersRulesCreateParams()
	otParams.SetName(*params.FilterName)
	otParams.SetInfo(emsFilterRuleAddParamsToONTAP(params))

	response, err := (*sc.api).EmsFiltersRulesCreate(otParams, nil)
	if err != nil {
		// Check if rule already exists (idempotent)
		if isAlreadyExistsError(err) {
			return nil // Ignore "already exists" errors
		}
		return err
	}

	if response == nil {
		return errors.New("unexpected response from EmsFiltersRulesCreate")
	}

	return nil
}

// EMSEventFilterRulesGet gets all rules for an EMS event filter
func (sc *supportClient) EMSEventFilterRulesGet(filterName string) ([]*EMSEventFilterRule, error) {
	if filterName == "" {
		return nil, errors.New("filter name is required")
	}

	otParams := support.NewEmsFilterRuleCollectionGetParams()
	otParams.SetName(filterName)
	response, err := (*sc.api).EmsFilterRuleCollectionGet(otParams, nil)
	if err != nil {
		return nil, err
	}
	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from EmsFilterRuleCollectionGet")
	}

	rules := make([]*EMSEventFilterRule, 0)
	for _, record := range response.Payload.EmsFilterRuleResponseInlineRecords {
		rule := &EMSEventFilterRule{
			Index: int(nillable.FromPointerWithFallback(record.Index, int64(0))), // ONTAP uses 1-based indexing
			Type:  nillable.FromPointerWithFallback(record.Type, ""),
		}
		if record.MessageCriteria != nil && record.MessageCriteria.Severities != nil {
			severitiesStr := *record.MessageCriteria.Severities
			rule.Severity = strings.Split(severitiesStr, ",")
		}
		rules = append(rules, rule)
	}
	return rules, nil
}

// EMSEventFilterRuleDelete deletes a rule from an EMS event filter by index
func (sc *supportClient) EMSEventFilterRuleDelete(filterName string, index int) error {
	if filterName == "" {
		return errors.New("filter name is required")
	}
	if index < 1 {
		return errors.New("rule index must be >= 1 (ONTAP uses 1-based indexing)")
	}

	otParams := support.NewEmsFilterRuleDeleteParams()
	otParams.SetName(filterName)
	indexStr := strconv.Itoa(index)
	otParams.SetIndex(indexStr)
	_, err := (*sc.api).EmsFilterRuleDelete(otParams, nil)
	if err != nil {
		// Check if rule doesn't exist (idempotent)
		if errors.IsNotFoundErr(err) {
			return nil // Ignore "not found" errors
		}
		return err
	}
	return nil
}

// EMSEventDestinationDelete deletes an EMS event destination
func (sc *supportClient) EMSEventDestinationDelete(name string) error {
	if name == "" {
		return errors.New("destination name is required")
	}

	otParams := support.NewEmsDestinationDeleteParams()
	otParams.SetName(name)
	_, err := (*sc.api).EmsDestinationDelete(otParams, nil)
	if err != nil {
		// Check if destination doesn't exist (idempotent)
		if errors.IsNotFoundErr(err) {
			return nil // Ignore "not found" errors
		}
		return err
	}
	return nil
}

// Conversion functions from our params to ONTAP client models

func emsDestinationCreateParamsToONTAP(params *EMSEventDestinationCreateParams) *models.EmsDestination {
	if params == nil {
		return nil
	}

	destination := &models.EmsDestination{
		Name: params.Name,
		Type: params.Type,
	}

	// For syslog type, set destination field to host and configure syslog
	if params.Type != nil && *params.Type == "syslog" && params.SyslogHost != nil {
		destination.Destination = params.SyslogHost

		syslogConfig := &models.EmsDestinationInlineSyslog{
			Port:      params.SyslogPort,
			Transport: params.SyslogTransport,
		}

		// Set format if timestamp or message format is specified
		if params.SyslogTimestampFormat != nil || params.SyslogMessageFormat != nil {
			format := &models.EmsDestinationInlineSyslogInlineFormat{}
			if params.SyslogTimestampFormat != nil {
				format.TimestampOverride = params.SyslogTimestampFormat
			}
			if params.SyslogMessageFormat != nil {
				format.Message = params.SyslogMessageFormat
			}
			syslogConfig.Format = format
		}

		destination.Syslog = syslogConfig
	}

	return destination
}

func emsDestinationModifyParamsToONTAP(params *EMSEventDestinationModifyParams) *models.EmsDestination {
	if params == nil {
		return nil
	}

	destination := &models.EmsDestination{}

	// Convert filter names to filter references
	if params.Filters != nil && len(params.Filters) > 0 {
		filters := make([]*models.EmsDestinationInlineFiltersInlineArrayItem, len(params.Filters))
		for i, filterName := range params.Filters {
			filterNameCopy := filterName
			filters[i] = &models.EmsDestinationInlineFiltersInlineArrayItem{
				Name: &filterNameCopy,
			}
		}
		destination.EmsDestinationInlineFilters = filters
	}

	return destination
}

func emsFilterCreateParamsToONTAP(params *EMSEventFilterCreateParams) *models.EmsFilter {
	if params == nil {
		return nil
	}

	return &models.EmsFilter{
		Name: params.Name,
	}
}

func emsFilterRuleAddParamsToONTAP(params *EMSEventFilterRuleAddParams) *models.EmsFilterRule {
	if params == nil {
		return nil
	}

	rule := &models.EmsFilterRule{
		Type: params.Type,
	}

	// Set message criteria with severities
	if params.Severity != nil && len(params.Severity) > 0 {
		// Convert severity array to comma-separated string
		severitiesStr := strings.Join(params.Severity, ",")
		rule.MessageCriteria = &models.EmsFilterRuleInlineMessageCriteria{
			Severities: &severitiesStr,
		}
	}

	return rule
}

// Conversion functions from ONTAP models to our models

func emsDestinationFromONTAP(dest *models.EmsDestination) *EMSEventDestination {
	if dest == nil {
		return nil
	}

	result := &EMSEventDestination{
		Name: nillable.FromPointerWithFallback(dest.Name, ""),
		Type: nillable.FromPointerWithFallback(dest.Type, ""),
	}

	if dest.Syslog != nil {
		result.Syslog = &EMSEventDestinationSyslog{
			Host:            nillable.FromPointerWithFallback(dest.Destination, ""),
			Port:            nillable.FromPointerWithFallback(dest.Syslog.Port, int64(0)),
			Transport:       nillable.FromPointerWithFallback(dest.Syslog.Transport, ""),
			TimestampFormat: "",
			MessageFormat:   "",
		}

		if dest.Syslog.Format != nil {
			result.Syslog.TimestampFormat = nillable.FromPointerWithFallback(dest.Syslog.Format.TimestampOverride, "")
			result.Syslog.MessageFormat = nillable.FromPointerWithFallback(dest.Syslog.Format.Message, "")
		}
	}

	return result
}

func emsFilterFromONTAP(filter *models.EmsFilter) *EMSEventFilter {
	if filter == nil {
		return nil
	}

	return &EMSEventFilter{
		Name: nillable.FromPointerWithFallback(filter.Name, ""),
	}
}

// Additional types for filter response
type EMSEventFilter struct {
	Name string
}

// EMSEventFilterRule represents an EMS filter rule
type EMSEventFilterRule struct {
	Index    int
	Type     string // "include", "exclude"
	Severity []string
}
