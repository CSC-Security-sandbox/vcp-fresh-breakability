package workflowquery

// JSON shape for vlm_config → cloud.ha_pair[].(vm1|vm2).lifs.(intercluster|nodemgmtinternal).ip
// as returned in Temporal child workflow results (CreateVSAClusterDeploymentWorkflow).
// Only fields required for IP extraction are declared; embedding keeps nesting clear.

type lifIPEmbed struct {
	IP string `json:"ip"`
}

type vmLifsIPEmbed struct {
	Lifs struct {
		Intercluster     lifIPEmbed `json:"intercluster"`
		Nodemgmtinternal lifIPEmbed `json:"nodemgmtinternal"`
	} `json:"lifs"`
}

type haPairIPEmbed struct {
	VM1 vmLifsIPEmbed `json:"vm1"`
	VM2 vmLifsIPEmbed `json:"vm2"`
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
