package vmrs

import (
    // testify - assert and mock packages for testing.
    "testing"

    "github.com/stretchr/testify/assert"
)

func TestParseConfig(t *testing.T) {
    cases := []struct {
        name           string
        configFilename string
        expectedError  string
        expectedConfig *VMRSConfig
    }{
        {
            name:           "ValidConfig",
            configFilename: "testdata/valid.yaml",
            expectedError:  "",
            expectedConfig: &VMRSConfig{
                HyperscalerPerfLimits: HyperscalerPerfLimits{
                    DiskPerfLimits: []DiskTypePerfLimit{
                        {
                            DiskType: "hyperdisk_balanced",
                            QualifiedVMs: []VMPerfLimit{
                                {
                                    VMType: "c4-standard-4",
                                    OntapLimits: OntapPerfLimit{
                                        IOPS:             1,
                                        ThroughputInMiBs: 2,
                                        CapacityInGiB:    3,
                                    },
                                    DiskLimits: DiskPerfLimit{
                                        IOPS:             4,
                                        ThroughputInMiBs: 5,
                                        CapacityInGiB:    6,
                                        MaxNumDisks:      7,
                                    },
                                },
                                {
                                    VMType: "c4-standard-8",
                                    OntapLimits: OntapPerfLimit{
                                        IOPS:             8,
                                        ThroughputInMiBs: 9,
                                        CapacityInGiB:    10,
                                    },
                                    DiskLimits: DiskPerfLimit{
                                        IOPS:             11,
                                        ThroughputInMiBs: 12,
                                        CapacityInGiB:    13,
                                        MaxNumDisks:      14,
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        {
            name:           "MissingFile",
            configFilename: "testdata/missing_file.yaml",
            expectedError: `[vmrs] ConfigParseError: failed to read config file due to error:  open testdata/missing_file.yaml: no such file or directory
 (path: testdata/missing_file.yaml)`,
            expectedConfig: nil,
        },
        {
            name:           "MissingDiskTypeField",
            configFilename: "testdata/missing_disk_type.yaml",
            expectedError: `[vmrs] ConfigParseError: failed to parse config file due to error:  [3:5] Key: 'DiskTypePerfLimit.DiskType' Error:Field validation for 'DiskType' failed on the 'required' tag
   1 | vmrs:
   2 |   disk_limits:
>  3 |     - qualified_vms:
           ^
   4 |       - vm_type: c4-standard-4
   5 |         ontap_perf:
   6 |           iops: 1
   7 |           
 (path: testdata/missing_disk_type.yaml)`,
            expectedConfig: nil,
        },
        {
            name:           "MissingDiskPerfLimitsField",
            configFilename: "testdata/missing_disk_perf.yaml",
            expectedError: `[vmrs] ConfigParseError: failed to parse config file due to error:  Key: 'VMPerfLimit.DiskLimits.IOPS' Error:Field validation for 'IOPS' failed on the 'required' tag
Key: 'VMPerfLimit.DiskLimits.ThroughputInMiBs' Error:Field validation for 'ThroughputInMiBs' failed on the 'required' tag
Key: 'VMPerfLimit.DiskLimits.CapacityInGiB' Error:Field validation for 'CapacityInGiB' failed on the 'required' tag
Key: 'VMPerfLimit.DiskLimits.MaxNumDisks' Error:Field validation for 'MaxNumDisks' failed on the 'required' tag
 (path: testdata/missing_disk_perf.yaml)`,
            expectedConfig: nil,
        },
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            config, err := LoadConfig(tc.configFilename)

            if tc.expectedError != "" {
                assert.NotNil(t, err)
                assert.EqualError(t, err, tc.expectedError)
            } else {
                assert.Nil(t, err)
                assert.Equal(t, tc.expectedConfig, config)
            }
        })
    }
}
