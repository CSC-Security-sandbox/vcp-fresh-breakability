// This file defines the configuration required for VMRS to make the right decisions.
//
// The configuration is parsed from a YAML file and provides methods to retrieve performance limits for different VM and disk types that are supported by various hyperscalers.

package vmrs

import (
    "fmt"
    "os"

    "github.com/go-playground/validator/v10"
    "github.com/goccy/go-yaml"
    "golang.org/x/exp/slog"
)

// VMRS configuration object that holds performance limits for different hyperscalers.
type VMRSConfig struct {
    // The list of performance limits - one element in the list for each hyperscaler.
    HyperscalerPerfLimits HyperscalerPerfLimits `yaml:"vmrs" validate:"required"`
}

// HyperscalerPerfLimits represents the performance limits for a specific hyperscaler.
type HyperscalerPerfLimits struct {
    // The list of performance limits for different disk types.
    DiskPerfLimits []DiskTypePerfLimit `yaml:"disk_limits" validate:"required,min=1"`
}

// DiskTypePerfLimit represents the performance limits for a specific disk type provided by the hyperscaler.
type DiskTypePerfLimit struct {
    // The type of the disk provided by the hyperscaler.
    DiskType string `yaml:"disk_type" validate:"required"`
    // Perf limits for qualified VMs.
    QualifiedVMs []VMPerfLimit `yaml:"qualified_vms" validate:"required,min=1"`
}

// The performance limits for a specific VM.
type VMPerfLimit struct {
    // The VM/instance type for which these limits apply.
    VMType string `yaml:"vm_type" validate:"required"`
    // The Ontap performance limits for this disk type.
    OntapLimits OntapPerfLimit `yaml:"ontap_perf" validate:"required"`
    // The performance limits specified by the hyperscaler for this disk type.
    DiskLimits DiskPerfLimit `yaml:"disk_perf" validate:"required"`
}

// OntapPerfLimit represents the performance limits for a specific VM type.
// This information is provided by our perf team.
type OntapPerfLimit struct {
    // The IOPSlimit for this VM type.
    IOPS int `yaml:"iops" validate:"required"`
    // The throughput limit in MiB/s for this VM type.
    ThroughputInMiBs int `yaml:"throughput_in_mibs" validate:"required"`
    // The capacity limit in GiB for this VM type.
    CapacityInGiB int `yaml:"capacity_in_gib" validate:"required"`
}

// DiskPerfLimit represents the performance limits for a specific VM type.
// This information is provided by the hyperscaler.
type DiskPerfLimit struct {
    // The IOPS limit for this disk/disk type.
    IOPS int `yaml:"iops" validate:"required"`
    // The throughput limit in MiB/s for this disk/disk type.
    ThroughputInMiBs int `yaml:"throughput_in_mibs" validate:"required"`
    // The capacity limit in GiB for this disk/disk type.
    CapacityInGiB int `yaml:"capacity_in_gib" validate:"required"`
    // The maximum number of disks that can be attached to this VM.
    MaxNumDisks int `yaml:"max_num_disks" validate:"required"`
}

// LoadConfig loads the VMRS configuration from the specified YAML file.
//
// Ideally, this function should be called once at the start of the application to load the configuration.
// It would seem like an ideal candidate to be invoked as part of package initialization. But, package initialization with side-effects is difficult to test, and can lead to unexpected behavior.
func LoadConfig(configFilePath string) (*VMRSConfig, error) {
    // Load the config file.
    file, err := os.Open(configFilePath)
    if err != nil {
        readErr := ConfigParseError{
            Message: fmt.Sprintln("failed to read config file due to error: ", err.Error()),
            Path:    configFilePath,
        }
        slog.Error(readErr.Error())
        return nil, &readErr
    }

    var config VMRSConfig

    // Unmarshal the YAML content into the VMRSConfig struct, and validate it.
    validate := validator.New()
    dec := yaml.NewDecoder(
        file,
        yaml.Validator(validate),
        yaml.Strict(),
    )
    err = dec.Decode(&config)
    if err != nil {
        parseErr := ConfigParseError{
            Message: fmt.Sprintln("failed to parse config file due to error: ", err.Error()),
            Path:    configFilePath,
        }
        slog.Error(parseErr.Error())
        return nil, &parseErr
    }

    return &config, nil
}
