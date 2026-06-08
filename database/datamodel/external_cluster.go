package datamodel

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

// ClusterAttributes holds metadata for an external cluster.
// ManagementIP may be supplied at onboard; ONTAP version is populated by the control plane later.
type ClusterAttributes struct {
	OntapVersion string `json:"ontap_version,omitempty"`
	ManagementIP string `json:"management_ip,omitempty"`
}

// Scan implements the sql.Scanner interface for ClusterAttributes.
func (a *ClusterAttributes) Scan(value interface{}) error {
	if value == nil {
		*a = ClusterAttributes{}
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, a)
}

// Value implements the driver.Valuer interface for ClusterAttributes.
func (a ClusterAttributes) Value() (driver.Value, error) {
	return json.Marshal(a)
}

// Cluster records an external (hardware) ONTAP cluster management endpoint.
type Cluster struct {
	BaseModel
	LocationID            string             `gorm:"column:location_id;index"`
	HostName              string             `gorm:"column:host_name"`
	Description           string             `gorm:"column:description;type:text"`
	Label                 string             `gorm:"column:label;type:text"`
	Protocol              string             `gorm:"column:protocol;default:'INSECURE_HTTPS'"`
	Port                  int                `gorm:"column:port"`
	AdminUsername         string             `gorm:"column:admin_username"`
	AdminPassword         string             `gorm:"column:admin_password"` // encrypted; never exposed via API
	LifecycleState        string             `gorm:"column:lifecycle_state;default:'CREATED'"`
	LifecycleStateDetails string             `gorm:"column:lifecycle_state_details;type:text"`
	ClusterAttributes     *ClusterAttributes `gorm:"column:onboard_attributes;type:jsonb"`
}

// TableName keeps the existing external_cluster_hosts table name.
func (Cluster) TableName() string {
	return "external_cluster_hosts"
}
