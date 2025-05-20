package vsa

import (
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func (rc *OntapRestProvider) GetInterclusterLIFs(servicePolicyName string) ([]*InterclusterLif, error) {
	client := getOntapClientFunc(rc.ClientParams)
	networkIPInterfacesGetParams := &ontaprest.NetworkIPInterfacesGetParams{BaseParams: ontaprest.BaseParams{Fields: []string{"ip.address"}}, ServicePolicyName: &servicePolicyName}
	icLif, err := client.Networking().InterclusterLifsGet(networkIPInterfacesGetParams)
	if err != nil {
		return nil, err
	}
	storageDataLifs := make([]*InterclusterLif, len(icLif))
	for ii, ontapIPInterface := range icLif {
		storageDataLif := &InterclusterLif{
			UUID:    nillable.FromPointer(ontapIPInterface.UUID),
			Name:    nillable.FromPointer(ontapIPInterface.Name),
			Address: nillable.FromPointer(ontapIPInterface.IP.Address),
		}
		storageDataLifs[ii] = storageDataLif
	}
	return storageDataLifs, nil
}
