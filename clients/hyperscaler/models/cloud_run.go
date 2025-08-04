package hyperscaler

type CloudRunServiceConfig struct {
	ProjectID    string
	LocationID   string
	ServiceName  string
	Image        string
	Description  string
	Labels       map[string]string
	Annotations  map[string]string
	EnvVars      map[string]string
	VolumeMounts []VolumeMount
	Volumes      []Volume
	Resources    *ResourceConfig
}

// VolumeMount represents a volume mount configuration
type VolumeMount struct {
	Name      string
	MountPath string
}

// Volume represents a volume configuration
type Volume struct {
	Name       string
	VolumeType string // "secret", "configmap", etc.
	Source     VolumeSource
}

// VolumeSource represents the source of a volume
type VolumeSource struct {
	SecretName string
	Items      []SecretItem
}

// SecretItem represents an item in a secret volume
type SecretItem struct {
	Path    string
	Version string
}

// ResourceConfig represents resource configuration for containers
type ResourceConfig struct {
	CPULimit    string
	MemoryLimit string
}

// CloudRunOperationResponse represents the response from a Cloud Run operation
type CloudRunOperationResponse struct {
	OperationName string
	Status        string
}
