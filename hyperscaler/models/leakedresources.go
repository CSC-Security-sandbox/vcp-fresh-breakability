package hyperscaler

// GCEInstance is the hyperscaler-leaf shape returned by InstanceLister.
// Field-for-field identical to vmscan.GCEInstanceItem; translation in
// scan_gce_instances_activity.go is a 1:1 copy. JSON tags are kept so the
// type prints/marshals usefully in logs without depending on the wire shape.
type GCEInstance struct {
	Project           string            `json:"project"`
	Zone              string            `json:"zone"`
	Name              string            `json:"name"`
	SelfLink          string            `json:"selfLink"`
	Status            string            `json:"status"`
	MachineType       string            `json:"machineType,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
}

// GCEDisk is the hyperscaler-leaf shape returned by DiskLister. Field-for-field
// identical to diskscan.GCEDiskItem (see GCEInstance for rationale).
type GCEDisk struct {
	Project           string            `json:"project"`
	Zone              string            `json:"zone"`
	Name              string            `json:"name"`
	SelfLink          string            `json:"selfLink"`
	Status            string            `json:"status"`
	SizeGB            int64             `json:"sizeGb"`
	Type              string            `json:"type,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
}
