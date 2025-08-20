package google

import (
	"fmt"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/dns/v1"
)

const (
	recordType = "A" // DNS record type for A records
)

// CreateResourceRecordSet creates a new DNS resource record set in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/create
func (gcpService *GcpServices) CreateResourceRecordSet(projectID, managedZone, ipAddress, recordName string) (*models.CustomCloudDNSRecord, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateResourceRecordSet for projectID : %s, managedZone : %s, recordName : %s", projectID, managedZone, recordName))

	rrs := &dns.ResourceRecordSet{
		Name:    recordName,
		Type:    recordType,
		Ttl:     env.CloudDNSCacheTTL,
		Rrdatas: []string{ipAddress},
	}
	resp, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.Create(projectID, managedZone, rrs).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to create resource record set: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError, err)
	}
	gcpService.Logger.Debugf("Resource record set created successfully: %s", resp.Name)
	return ValidateAndConvertToCustomCloudDNSRecord(resp, managedZone)
}

// GetResourceRecordSet retrieves a DNS resource record set by its name and type in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/get
func (gcpService *GcpServices) GetResourceRecordSet(projectID, managedZone, recordName string) (*models.CustomCloudDNSRecord, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetResourceRecordSet for projectID : %s, managedZone : %s, recordName : %s", projectID, managedZone, recordName))

	resp, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.List(projectID, managedZone).Name(recordName).Type(recordType).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get resource record set: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceFetchError, err)
	}
	if resp == nil || len(resp.Rrsets) == 0 {
		gcpService.Logger.Errorf("resource record set not found for recordName : %s, recordType : %s", recordName, recordType)
		return nil, nil // Return nil record and nil err if no record set is found
	}
	gcpService.Logger.Debugf("Resource record set got successfully: %v", resp.Rrsets[0])

	return ValidateAndConvertToCustomCloudDNSRecord(resp.Rrsets[0], managedZone)
}

// DeleteResourceRecordSet deletes a DNS resource record set by its name and type in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/delete
func (gcpService *GcpServices) DeleteResourceRecordSet(projectID, managedZone, recordName string) error {
	gcpService.Logger.Debug(fmt.Sprintf("Calling DeleteResourceRecordSet for projectID : %s, managedZone : %s, recordName : %s", projectID, managedZone, recordName))

	_, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.Delete(projectID, managedZone, recordName, recordType).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to delete resource record set: %v", err)
		return vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceDeprovisionError, err)
	}
	gcpService.Logger.Debugf("Resource record set deleted successfully: %s", recordName)
	return nil
}
