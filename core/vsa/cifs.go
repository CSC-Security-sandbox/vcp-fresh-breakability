package vsa

import (
	"fmt"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

// GetCIFSService retrieves CIFS service configuration for the specified SVM
func (rc *OntapRestProvider) GetCIFSService(svmName, externalSVMUUID string) (*ontapRest.CifsService, error) {
	client, err := rc.CreateRESTClient()
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get ONTAP client: %w", err))
	}

	cifs, err := client.NAS().CifsServiceGet(&ontapRest.CifsServiceGetParams{
		SvmUUID: &externalSVMUUID,
		SvmName: &svmName,
		BaseParams: ontapRest.BaseParams{Fields: []string{
			"ad_domain",
			"name",
		}},
	})
	if err != nil {
		return nil, err
	}
	return cifs, nil
}
