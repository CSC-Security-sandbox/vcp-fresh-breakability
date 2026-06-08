package gcp

import (
	"context"
	"fmt"
	"strings"

	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// OnboardExternalClusters persists external cluster records (synchronous; no job or LRO).
func (o *GCPOrchestrator) OnboardExternalClusters(ctx context.Context, params *commonparams.OnboardExternalClustersParams) ([]*datamodel.Cluster, error) {
	if params == nil || len(params.Hosts) == 0 {
		return nil, customerrors.NewBadRequestErr("at least one host is required")
	}
	if strings.TrimSpace(params.LocationID) == "" {
		return nil, customerrors.NewBadRequestErr("locationId is required")
	}

	created := make([]*datamodel.Cluster, 0, len(params.Hosts))

	for _, h := range params.Hosts {
		if strings.TrimSpace(h.HostName) == "" || strings.TrimSpace(h.Username) == "" || strings.TrimSpace(h.Password) == "" {
			return nil, customerrors.NewBadRequestErr("hostName, username, and password are required for each host")
		}
		if strings.TrimSpace(h.ManagementIP) == "" {
			return nil, customerrors.NewBadRequestErr("managementIp is required for each host")
		}

		encryptedPassword, err := utils.EncryptPassword(log.Secret(h.Password))
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt admin password for host %q: %w", h.HostName, err)
		}

		protocol, port, normErr := datamodel.NormalizeExternalClusterProtocolAndPort(h.Protocol, h.Port)
		if normErr != nil {
			return nil, customerrors.NewBadRequestErr(normErr.Error())
		}

		onboardAttrs := &datamodel.ClusterAttributes{
			ManagementIP: h.ManagementIP,
		}

		row := &datamodel.Cluster{
			LocationID:            params.LocationID,
			HostName:              h.HostName,
			Description:           h.Description,
			Label:                 h.Label,
			Protocol:              protocol,
			Port:                  port,
			AdminUsername:         h.Username,
			AdminPassword:         *encryptedPassword,
			LifecycleState:        database.ExternalClusterStateCreated,
			LifecycleStateDetails: "Registered",
			ClusterAttributes:     onboardAttrs,
		}

		inserted, err := o.storage.CreateExternalCluster(ctx, row)
		if err != nil {
			return nil, err
		}
		created = append(created, inserted)
	}

	return created, nil
}

// GetExternalCluster returns a single external cluster by UUID.
func (o *GCPOrchestrator) GetExternalCluster(ctx context.Context, externalClusterID string) (*datamodel.Cluster, error) {
	return o.storage.GetExternalCluster(ctx, externalClusterID)
}

// UpdateExternalCluster applies mutable field updates to an external cluster.
func (o *GCPOrchestrator) UpdateExternalCluster(ctx context.Context, params *commonparams.UpdateExternalClusterParams) (*datamodel.Cluster, error) {
	if params == nil || !params.HasUpdates() {
		return nil, customerrors.NewBadRequestErr("at least one field must be provided to update")
	}

	existing, err := o.storage.GetExternalCluster(ctx, params.ExternalClusterID)
	if err != nil {
		return nil, err
	}

	if params.Description != nil {
		existing.Description = *params.Description
	}
	if params.Label != nil {
		existing.Label = *params.Label
	}
	if params.ManagementIP != nil {
		if existing.ClusterAttributes == nil {
			existing.ClusterAttributes = &datamodel.ClusterAttributes{}
		}
		existing.ClusterAttributes.ManagementIP = *params.ManagementIP
	}
	if params.Username != nil {
		existing.AdminUsername = *params.Username
	}
	if params.Password != nil {
		encryptedPassword, encErr := utils.EncryptPassword(log.Secret(*params.Password))
		if encErr != nil {
			return nil, fmt.Errorf("failed to encrypt admin password: %w", encErr)
		}
		existing.AdminPassword = *encryptedPassword
	}

	protocol := existing.Protocol
	port := existing.Port
	if params.Protocol != nil {
		protocol = *params.Protocol
	}
	if params.Port != nil {
		port = *params.Port
	} else if params.Protocol != nil {
		// Protocol-only update: apply the default port for the new protocol (same as onboard).
		port = 0
	}
	if params.Protocol != nil || params.Port != nil {
		normalizedProtocol, normalizedPort, normErr := datamodel.NormalizeExternalClusterProtocolAndPort(protocol, port)
		if normErr != nil {
			return nil, customerrors.NewBadRequestErr(normErr.Error())
		}
		existing.Protocol = normalizedProtocol
		existing.Port = normalizedPort
	}

	return o.storage.UpdateExternalCluster(ctx, existing)
}

// DeleteExternalCluster soft-deletes an external cluster by UUID.
func (o *GCPOrchestrator) DeleteExternalCluster(ctx context.Context, externalClusterID string) (*datamodel.Cluster, error) {
	return o.storage.DeleteExternalCluster(ctx, externalClusterID)
}
