package vsa

import (
	"strings"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// IgroupCreate creates an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupCreate(params IgroupCreateParams) (string, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return "", err
	}
	iGroupName, err := client.SAN().IGroupCreate(&ontapRest.IgroupCreateParams{
		Name:       params.IgroupName,
		SvmName:    params.SvmName,
		OsType:     params.OsType,
		Initiators: params.Initiator,
	})
	if err != nil {
		return "", vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	return iGroupName, nil
}

// IgroupDelete deletes an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupDelete(uuid string) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	err = client.SAN().IGroupDelete(&ontapRest.IgroupDeleteParams{
		UUID: uuid,
	})
	if err != nil && !strings.Contains(err.Error(), "was not found") {
		return err
	}
	return nil
}

// IgroupGet creates an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupGet(name, svmName *string) (*ontapRest.Igroup, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	iGroup, err := client.SAN().IGroupGet(&ontapRest.IgroupGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"initiators"},
		},
		Name:    name,
		SvmName: svmName,
	})
	if err != nil {
		return nil, err
	}
	return iGroup, nil
}

func (rc *OntapRestProvider) IgroupExists(name string, svm *string) (bool, *ontapRest.Igroup, error) {
	res, err := rc.IgroupGet(&name, svm)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, nil, nil
		}
		return false, nil, vsaerrors.NewVCPError(vsaerrors.ErrOntapRestAPIError, err)
	}
	if res == nil {
		return false, nil, nil
	}

	return true, res, nil
}

func (rc *OntapRestProvider) IgroupAddInitiator(params IgroupAddInitiator) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	err = client.SAN().IGroupAddInitiator(&ontapRest.IgroupAddInitiatorParams{
		InitiatorQNs: params.Initiator,
		IgroupUUID:   params.IgroupUUID,
	})

	if err != nil && !strings.Contains(err.Error(), "already contains initiator") {
		return err
	}

	return nil
}

func (rc *OntapRestProvider) IgroupDeleteInitiator(params IgroupDeleteInitiator) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	err = client.SAN().IGroupDeleteInitiator(&ontapRest.IgroupDeleteInitiatorParams{
		InitiatorIQNName: params.InitiatorName,
		IgroupUUID:       params.IgroupUUID,
	})

	if err != nil && !strings.Contains(err.Error(), "does not contain initiator") {
		return err
	}

	return nil
}
