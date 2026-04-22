package workflowquery

// JSON shape for vlm_config -> cloud.ha_pair[].(vm1|vm2).lifs.(intercluster|nodemgmtinternal).ip
// as returned in Temporal child workflow results (CreateVSAClusterDeploymentWorkflow).
// Only fields required for VM metadata extraction are declared; embedding keeps nesting clear.

type lifIPEmbed struct {
	IP string `json:"ip"`
}

type vmMetadata struct {
	Name            string `json:"name"`
	SerialNumber    string `json:"serial_number"`
	VSAManagementIP string `json:"vsa_management_ip"`
	Lifs            struct {
		Intercluster     lifIPEmbed `json:"intercluster"`
		Nodemgmtinternal lifIPEmbed `json:"nodemgmtinternal"`
	} `json:"lifs"`
}

type haPairIPEmbed struct {
	VM1 vmMetadata `json:"vm1"`
	VM2 vmMetadata `json:"vm2"`
}

// vlmConfigIPEmbed is the minimal vlm_config subtree unmarshaled from workflow result JSON.
type vlmConfigIPEmbed struct {
	Cloud struct {
		HAPairs []haPairIPEmbed `json:"ha_pair"`
	} `json:"cloud"`
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
		vm.Lifs.Intercluster.IP == "" && vm.Lifs.Nodemgmtinternal.IP == ""
}

func poolVMMetadataFromEmbed(cfg *vlmConfigIPEmbed) []OCICreatePoolVMMetadata {
	if cfg == nil {
		return nil
	}

	vms := make([]OCICreatePoolVMMetadata, 0, len(cfg.Cloud.HAPairs)*2)
	for _, pair := range cfg.Cloud.HAPairs {
		if !vmMetadataIsEmpty(pair.VM1) {
			vms = append(vms, OCICreatePoolVMMetadata{
				Name:            pair.VM1.Name,
				SerialNumber:    pair.VM1.SerialNumber,
				VSAManagementIP: pair.VM1.VSAManagementIP,
				InterclusterIP:  pair.VM1.Lifs.Intercluster.IP,
				NodeIP:          pair.VM1.Lifs.Nodemgmtinternal.IP,
			})
		}
		if !vmMetadataIsEmpty(pair.VM2) {
			vms = append(vms, OCICreatePoolVMMetadata{
				Name:            pair.VM2.Name,
				SerialNumber:    pair.VM2.SerialNumber,
				VSAManagementIP: pair.VM2.VSAManagementIP,
				InterclusterIP:  pair.VM2.Lifs.Intercluster.IP,
				NodeIP:          pair.VM2.Lifs.Nodemgmtinternal.IP,
			})
		}
	}
	return vms
}
