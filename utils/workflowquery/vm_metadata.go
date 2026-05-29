package workflowquery

import "fmt"

// 1 GBps = 10^9 bytes/s
// 1 MiBps = 2^20 bytes/s
// MiBpsPerGBps = 10^9 / 2^20 = 953.67431640625
const MiBpsPerGBps = 953.67431640625

// haPairLabel converts a zero-based slice index into the public 1-indexed
// HA-pair label exposed on the API (ha_pair-1, ha_pair-2, ...). Callers
// pass the natural slice index from `cloud.ha_pair`; the +1 lives here so
// the wire format stays 1-indexed regardless of how callers iterate.
func haPairLabel(i int) string {
	return fmt.Sprintf("ha_pair-%d", i+1)
}

type lifIPEmbed struct {
	IP string `json:"ip"`
}
type dataDisk struct {
	Size           int64 `json:"size"`
	DiskIOPS       int64 `json:"disk_iops"`
	DiskThroughput int64 `json:"disk_throughput"`
}

type vmMetadata struct {
	Name            string `json:"name"`
	SerialNumber    string `json:"serial_number"`
	VSAManagementIP string `json:"vsa_management_ip"`
	Lifs            struct {
		Intercluster     lifIPEmbed `json:"intercluster"`
		Nodemgmtinternal lifIPEmbed `json:"nodemgmtinternal"`
		Rbac             lifIPEmbed `json:"rbac"`
	} `json:"lifs"`
	DataDisks []dataDisk `json:"data_disks"`
}

type haPairIPEmbed struct {
	VM1 vmMetadata `json:"vm1"`
	VM2 vmMetadata `json:"vm2"`
}

type vlmConfigIPEmbed struct {
	Cloud struct {
		HAPairs []haPairIPEmbed `json:"ha_pair"`
	} `json:"cloud"`
	Deployment struct {
		Labels map[string]string `json:"labels"`
	} `json:"deployment"`
}

func poolUUIDFromEmbed(cfg *vlmConfigIPEmbed) string {
	if cfg == nil {
		return ""
	}
	return cfg.Deployment.Labels["pool_uuid"]
}

// poolOCIDFromEmbed pulls the OCI pool resource OCID out of the VLM config's
// deployment labels. Returns "" when cfg is nil or the label is missing/empty,
// so callers can rely on the `omitempty` JSON tag to drop the field from the
// poolMetadata response when the value is unavailable.
func poolOCIDFromEmbed(cfg *vlmConfigIPEmbed) string {
	if cfg == nil {
		return ""
	}
	return cfg.Deployment.Labels["pool_ocid"]
}

func interclusterIPsFromEmbed(cfg *vlmConfigIPEmbed) []string {
	if cfg == nil {
		return nil
	}
	var ips []string
	seen := make(map[string]struct{})
	add := func(ip string) {
		if ip == "" {
			return
		}
		if _, ok := seen[ip]; ok {
			return
		}
		seen[ip] = struct{}{}
		ips = append(ips, ip)
	}
	for _, pair := range cfg.Cloud.HAPairs {
		add(pair.VM1.Lifs.Intercluster.IP)
		add(pair.VM2.Lifs.Intercluster.IP)
	}
	return ips
}

func clusterIPFromEmbed(cfg *vlmConfigIPEmbed) string {
	if cfg == nil {
		return ""
	}
	for _, pair := range cfg.Cloud.HAPairs {
		if ip := pair.VM1.Lifs.Rbac.IP; ip != "" {
			return ip
		}
		if ip := pair.VM2.Lifs.Rbac.IP; ip != "" {
			return ip
		}
	}
	return ""
}

func nodemgmtInternalIPsFromEmbed(cfg *vlmConfigIPEmbed) []string {
	if cfg == nil {
		return nil
	}
	var ips []string
	seen := make(map[string]struct{})
	add := func(ip string) {
		if ip == "" {
			return
		}
		if _, ok := seen[ip]; ok {
			return
		}
		seen[ip] = struct{}{}
		ips = append(ips, ip)
	}
	for _, pair := range cfg.Cloud.HAPairs {
		add(pair.VM1.Lifs.Nodemgmtinternal.IP)
		add(pair.VM2.Lifs.Nodemgmtinternal.IP)
	}
	return ips
}

func vmMetadataIsEmpty(vm vmMetadata) bool {
	return vm.Name == "" && vm.SerialNumber == "" && vm.VSAManagementIP == "" &&
		vm.Lifs.Intercluster.IP == "" && vm.Lifs.Nodemgmtinternal.IP == "" &&
		len(vm.DataDisks) == 0
}

func dataDiskTotals(disks []dataDisk) (sizeInGiB, iops int64, throughputGBps float64) {
	var throughputMiBps int64
	for _, d := range disks {
		sizeInGiB += d.Size
		iops += d.DiskIOPS
		throughputMiBps += d.DiskThroughput
	}
	throughputGBps = float64(throughputMiBps) / MiBpsPerGBps
	return
}

func poolVMMetadataFromEmbed(cfg *vlmConfigIPEmbed) []OCICreatePoolVMMetadata {
	if cfg == nil {
		return nil
	}

	vms := make([]OCICreatePoolVMMetadata, 0, len(cfg.Cloud.HAPairs)*2)
	for i, pair := range cfg.Cloud.HAPairs {
		label := haPairLabel(i)
		if !vmMetadataIsEmpty(pair.VM1) {
			sizeInGiB, iops, throughputGBps := dataDiskTotals(pair.VM1.DataDisks)
			vms = append(vms, OCICreatePoolVMMetadata{
				Name:            pair.VM1.Name,
				SerialNumber:    pair.VM1.SerialNumber,
				VSAManagementIP: pair.VM1.VSAManagementIP,
				InterclusterIP:  pair.VM1.Lifs.Intercluster.IP,
				HAPair:          label,
				SizeInGiB:       sizeInGiB,
				IOPS:            iops,
				ThroughputGBps:  throughputGBps,
			})
		}
		if !vmMetadataIsEmpty(pair.VM2) {
			sizeInGiB, iops, throughputGBps := dataDiskTotals(pair.VM2.DataDisks)
			vms = append(vms, OCICreatePoolVMMetadata{
				Name:            pair.VM2.Name,
				SerialNumber:    pair.VM2.SerialNumber,
				VSAManagementIP: pair.VM2.VSAManagementIP,
				InterclusterIP:  pair.VM2.Lifs.Intercluster.IP,
				HAPair:          label,
				SizeInGiB:       sizeInGiB,
				IOPS:            iops,
				ThroughputGBps:  throughputGBps,
			})
		}
	}
	return vms
}
