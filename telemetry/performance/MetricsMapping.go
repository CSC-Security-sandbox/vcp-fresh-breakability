package performance

type Triple struct {
	Left   string
	Middle string
	Right  string
}

type VSAHarvestMetric int

const (
	VolumeSpaceLogicalUsed VSAHarvestMetric = iota
)

func GetMetricsMapping() map[VSAHarvestMetric]Triple {
	metricsMappingMap := map[VSAHarvestMetric]Triple{
		VolumeSpaceLogicalUsed: {
			Left: "bytes_used", Middle: "", Right: "",
		},
	}
	return metricsMappingMap
}

func (v VSAHarvestMetric) String() string {
	switch v {
	case VolumeSpaceLogicalUsed:
		return "volume_space_logical_used"
	default:
		return "unknown"
	}
}
