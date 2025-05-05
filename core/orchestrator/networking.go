package orchestrator

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"google.golang.org/api/servicenetworking/v1"
)

var (
	region = env.GetString("REGION", "")
)

// FindTenancyAndGetSubnetwork finds the tenancy unit and creates a subnetwork for the tenant project
func FindTenancyAndGetSubnetwork(ctx context.Context, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*string, *servicenetworking.Subnetwork, error) {
	// need to pass tenantProjectRegion only in case of CBR where region != the regional region as set from env variable
	var gService hyperscaler.GoogleServices
	gcpService := &google.GcpServices{
		Ctx:    ctx,
		Logger: ctx.Value(middleware.ContextSLoggerKey).(log.Logger),
	}
	gService = gcpService

	gcpService.Logger.Debug("gcpService initialized")

	if tenantProjectRegion == nil {
		tenantProjectRegion = &region
	}
	gcpService.Logger.Debug("Calling InitializeClients")
	err := gService.InitializeClients()
	if err != nil || !gService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, nil, errors.New("Initialisation of service failed")
	}

	tenantProjectNumber, err := gService.GetTenantProject(consumerVPC, customerProjectNumber, *tenantProjectRegion)
	if err != nil {
		gcpService.Logger.Errorf("Error finding tenancy unit: %v", err)
		return nil, nil, err
	}
	subnet, err := gService.CreateSubnetwork(consumerVPC, *tenantProjectRegion, tenantProjectNumber)
	if err != nil {
		gcpService.Logger.Errorf("Error adding subnetwork: %v", err)
		return nil, nil, err
	}
	gcpService.Logger.Errorf("FindTenancyAndGetSubnetwork: tenantProjectNumber :  %s subnet  :  %s   ", &tenantProjectNumber, subnet)
	return &tenantProjectNumber, subnet, nil
}
