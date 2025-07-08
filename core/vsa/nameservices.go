package vsa

import ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"

func (rc *OntapRestProvider) CreateDns(params CreateDnsParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	dnsCreateParams := &ontapRest.DNSCreateParams{
		Domains:    params.Domains,
		DNSServers: params.Servers,
	}
	_, err = client.NameServices().DnsCreate(dnsCreateParams)
	if err != nil {
		return err
	}
	return nil
}
