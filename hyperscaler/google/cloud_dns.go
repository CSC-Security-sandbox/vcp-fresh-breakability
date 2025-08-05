package google

import (
	"fmt"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/dns/v1"
)

const (
	recordType = "A" // DNS record type for A records
)

// CreateResourceRecordSet creates a new DNS resource record set in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/create
func (gcpService *GcpServices) CreateResourceRecordSet(projectID, managedZone, ipAddress, recordName string) (*models.CustomCloudDNSRecord, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling CreateResourceRecordSet for project name : %s, managedZone : %s", projectID, managedZone))

	rrs := &dns.ResourceRecordSet{
		Name:    recordName,
		Type:    recordType,
		Ttl:     env.CloudDNSCacheTTL,
		Rrdatas: []string{ipAddress},
	}
	resp, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.Create(projectID, managedZone, rrs).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to create resource record set: %v", err)
		return nil, err
	}
	gcpService.Logger.Debugf("Resource record set created successfully: %s", resp.Name)
	return ValidateAndConvertToCustomCloudDNSRecord(resp, managedZone)
}

// GetResourceRecordSet retrieves a DNS resource record set by its name and type in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/get
func (gcpService *GcpServices) GetResourceRecordSet(projectID, managedZone, recordName string) (*models.CustomCloudDNSRecord, error) {
	gcpService.Logger.Debug(fmt.Sprintf("Calling GetResourceRecordSet for project recordName : %s, managedZone : %s, recordName : %s", projectID, managedZone, recordName))

	resp, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.List(projectID, managedZone).Name(recordName).Type(recordType).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to get resource record set: %v", err)
		return nil, err
	}
	if len(resp.Rrsets) == 0 {
		return nil, fmt.Errorf("no DNS records found for recordName : %s, recordType : %s", recordName, recordType)
	}
	gcpService.Logger.Debugf("Resource record set got successfully: %v", resp.Rrsets[0])

	return ValidateAndConvertToCustomCloudDNSRecord(resp.Rrsets[0], managedZone)
}

// DeleteResourceRecordSet deletes a DNS resource record set by its name and type in the specified managed zone. Reference : https://cloud.google.com/dns/docs/reference/rest/v1/resourceRecordSets/delete
func (gcpService *GcpServices) DeleteResourceRecordSet(projectID, managedZone, recordName string) error {
	gcpService.Logger.Debug(fmt.Sprintf("Calling DeleteResourceRecordSet for project name : %s, managedZone : %s", projectID, managedZone))

	_, err := gcpService.AdminGCPService.cloudDnsService.ResourceRecordSets.Delete(projectID, managedZone, recordName, recordType).Context(gcpService.Ctx).Do()
	if err != nil {
		gcpService.Logger.Errorf("Failed to delete resource record set: %v", err)
		return err
	}
	gcpService.Logger.Debugf("Resource record set deleted successfully: %s", recordName)
	return nil
}
