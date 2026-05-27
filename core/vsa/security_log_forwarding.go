package vsa

import (
	"fmt"
	"strings"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (rc *OntapRestProvider) CreateSecurityLogForwarding(params CreateSecurityLogForwardingParams) (*CreateSecurityLogForwardingResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	logForwarding, err := client.Security().SecurityLogForwardingCreate(&ontapRest.SecurityLogForwardingCreateParams{
		Address:      params.Address,
		Protocol:     params.Protocol,
		Port:         params.Port,
		Facility:     params.Facility,
		VerifyServer: params.VerifyServer,
	})

	if err != nil {
		return nil, err
	}

	if logForwarding != nil {
		response := &CreateSecurityLogForwardingResponse{ProviderResponse: ProviderResponse{Name: *logForwarding[0].Address}}
		return response, nil
	}
	return nil, err
}

func (rc *OntapRestProvider) GetSecurityLogForwarding(params GetSecurityLogForwardingParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	_, err = client.Security().SecurityLogForwardingGet(&ontapRest.SecurityLogForwardingGetParams{
		Address: params.Address,
		Port:    params.Port,
	})

	if err != nil {
		return err
	}

	return nil
}

func (rc *OntapRestProvider) CreateEMSEventForwarding(params CreateEMSEventForwardingParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	supportClient := client.Support()

	// Step 1: Create event notification destination
	destinationParams := &ontapRest.EMSEventDestinationCreateParams{
		Name:                  nillable.GetStringPtr(params.DestinationName),
		Type:                  nillable.GetStringPtr("syslog"),
		SyslogHost:            nillable.GetStringPtr(params.DestinationIP),
		SyslogPort:            nillable.GetInt64Ptr(params.DestinationPort),
		SyslogTransport:       nillable.GetStringPtr(params.Transport),
		SyslogTimestampFormat: nillable.GetStringPtr(params.TimestampFormat),
		SyslogMessageFormat:   nillable.GetStringPtr(params.MessageFormat),
	}

	if err := supportClient.EMSEventDestinationCreate(destinationParams); err != nil {
		// Error already handled in SupportClient (idempotent)
		if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "983") {
			return fmt.Errorf("failed to create EMS destination: %w", err)
		}
		rc.Logger.Infof("EMS destination %s already exists, continuing", params.DestinationName)
	}

	// Step 2: Create event filter
	filterParams := &ontapRest.EMSEventFilterCreateParams{
		Name: nillable.GetStringPtr(params.FilterName),
	}

	if err := supportClient.EMSEventFilterCreate(filterParams); err != nil {
		// Error already handled in SupportClient (idempotent)
		if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "983") {
			return fmt.Errorf("failed to create EMS filter: %w", err)
		}
		rc.Logger.Infof("EMS filter %s already exists, continuing", params.FilterName)
	}

	// Step 3: Add filter rules
	ruleParams := &ontapRest.EMSEventFilterRuleAddParams{
		FilterName: nillable.GetStringPtr(params.FilterName),
		Type:       nillable.GetStringPtr("include"),
		Severity:   params.Severities,
	}

	if err := supportClient.EMSEventFilterRuleAdd(ruleParams); err != nil {
		// Error already handled in SupportClient (idempotent)
		if !strings.Contains(err.Error(), "already exists") && !strings.Contains(err.Error(), "983") {
			return fmt.Errorf("failed to add EMS filter rule: %w", err)
		}
		rc.Logger.Infof("EMS filter rule already exists, continuing")
	}

	// Step 4: Link filter to destination
	modifyParams := &ontapRest.EMSEventDestinationModifyParams{
		Filters: []string{params.FilterName},
	}

	if err := supportClient.EMSEventDestinationModify(params.DestinationName, modifyParams); err != nil {
		// Error already handled in SupportClient (idempotent)
		if !strings.Contains(err.Error(), "already") && !strings.Contains(err.Error(), "983") {
			return fmt.Errorf("failed to link EMS filter to destination: %w", err)
		}
		rc.Logger.Infof("EMS filter already linked to destination, continuing")
	}

	return nil
}

func (rc *OntapRestProvider) GetEMSEventForwarding(destinationName string) (*EMSEventDestination, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	supportClient := client.Support()
	destination, err := supportClient.EMSEventDestinationGet(destinationName)
	if err != nil {
		return nil, err
	}

	if destination == nil {
		return nil, fmt.Errorf("EMS destination %s not found", destinationName)
	}

	response := &EMSEventDestination{
		Name: destination.Name,
		Type: destination.Type,
	}

	if destination.Syslog != nil {
		response.Syslog = &EMSEventDestinationSyslog{
			Host:            destination.Syslog.Host,
			Port:            destination.Syslog.Port,
			Transport:       destination.Syslog.Transport,
			TimestampFormat: destination.Syslog.TimestampFormat,
			MessageFormat:   destination.Syslog.MessageFormat,
		}
	}

	return response, nil
}

// DeleteEMSEventForwarding deletes EMS event forwarding configuration
// It removes the destination, filter, and all associated rules in reverse order of creation
func (rc *OntapRestProvider) DeleteEMSEventForwarding(destinationName, filterName string) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	supportClient := client.Support()

	// Step 1: Unlink filter from destination (set filters to empty list)
	// This must be done before deleting the filter
	modifyParams := &ontapRest.EMSEventDestinationModifyParams{
		Filters: []string{}, // Empty list to unlink all filters
	}

	if err := supportClient.EMSEventDestinationModify(destinationName, modifyParams); err != nil {
		// If destination doesn't exist, that's okay (idempotent)
		if !strings.Contains(strings.ToLower(err.Error()), "not found") {
			rc.Logger.Warnf("Failed to unlink filter from destination %s: %v (continuing with cleanup)", destinationName, err)
		}
	}

	// Step 2: Delete all filter rules
	// Get all rules first, then delete them in reverse order (to maintain indices)
	if filterName != "" {
		rules, err := supportClient.EMSEventFilterRulesGet(filterName)
		if err != nil {
			// If filter doesn't exist, rules don't exist either - that's okay
			if !strings.Contains(strings.ToLower(err.Error()), "not found") && !strings.Contains(err.Error(), "needs to be generated") {
				rc.Logger.Warnf("Failed to get filter rules for %s: %v (continuing with cleanup)", filterName, err)
			}
		} else {
			// Delete rules in reverse order to avoid index shifting issues
			for i := len(rules) - 1; i >= 0; i-- {
				if err := supportClient.EMSEventFilterRuleDelete(filterName, rules[i].Index); err != nil {
					// If rule doesn't exist, that's okay (idempotent)
					if !strings.Contains(strings.ToLower(err.Error()), "not found") && !strings.Contains(err.Error(), "needs to be generated") {
						rc.Logger.Warnf("Failed to delete filter rule at index %d: %v (continuing with cleanup)", rules[i].Index, err)
					}
				}
			}
		}

		// Step 3: Delete the filter
		if err := supportClient.EMSEventFilterDelete(filterName); err != nil {
			// If filter doesn't exist, that's okay (idempotent)
			if !strings.Contains(strings.ToLower(err.Error()), "not found") && !strings.Contains(err.Error(), "needs to be generated") {
				rc.Logger.Warnf("Failed to delete EMS filter %s: %v (continuing with cleanup)", filterName, err)
			}
		}
	}

	// Step 4: Delete the destination
	if err := supportClient.EMSEventDestinationDelete(destinationName); err != nil {
		// If destination doesn't exist, that's okay (idempotent)
		if !strings.Contains(strings.ToLower(err.Error()), "not found") && !strings.Contains(err.Error(), "needs to be generated") {
			return fmt.Errorf("failed to delete EMS destination %s: %w", destinationName, err)
		}
	}

	return nil
}
