package vsa

import ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"

func (rc *OntapRestProvider) CreateNetworkIpRoute(params CreateNetworkIPRouteParams) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Networking().NetworkIPRouteCreateDefault(&ontapRest.NetworkIPDefaultRouteCreateParams{
		IPSpace: ipSpaceName,
		SvmName: params.SvmName,
		Gateway: params.Gateway,
	})
	return err
}
