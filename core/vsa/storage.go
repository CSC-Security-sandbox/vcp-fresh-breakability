package vsa

import (
	"fmt"
	"strings"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

var (
	getOntapClientFunc = getOntapClient
)

func getOntapClient(clientParams ontapRest.RESTClientParams) ontapRest.RESTClient {
	return ontapRest.NewOntapRestClient(clientParams)
}

func (rc *OntapRestProvider) IsAggregateOnline(aggregateName string) (bool, error) {
	client := getOntapClientFunc(rc.ClientParams)
	aggr, err := client.Storage().AggregateFindByName(&ontapRest.AggregateCollectionGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"state"},
		},
		Name: &aggregateName,
	})
	if err != nil {
		return false, err
	}
	if aggr == nil {
		return false, nil
	}
	return aggr.IsOnline(), nil
}

func (rc *OntapRestProvider) GetAggregateByName(name string) (*Aggregate, error) {
	client := getOntapClientFunc(rc.ClientParams)
	aggr, err := client.Storage().AggregateFindByName(&ontapRest.AggregateCollectionGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"uuid", "state"},
		},
		Name: &name,
	})
	if err != nil {
		return nil, err
	}
	if aggr == nil {
		return nil, fmt.Errorf("aggregate not found")
	}
	return &Aggregate{
		Name:  *aggr.Name,
		State: *aggr.State,
	}, nil
}

// LunCreate creates a LUN by calling the ONTAP REST Client
func (rc *OntapRestProvider) LunCreate(params LunCreateParams) (*LunResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	lun, err := client.SAN().LunCreate(&ontapRest.LunCreateParams{
		Name:                           params.LunName,
		SvmName:                        params.SvmName,
		OsType:                         params.OsType,
		VolumeName:                     params.VolumeName,
		Size:                           params.Size,
		ThinProvisioningSupportEnabled: nillable.ToPointer(true),
	})
	if err != nil {
		if strings.Contains(err.Error(), "A LUN or NVMe namespace already exists") {
			return nil, errors.NewConflictErr(fmt.Sprintf("LUN %s already exists in SVM %s", params.LunName, params.SvmName))
		}
		return nil, err
	}
	return &LunResponse{
		ProviderResponse: ProviderResponse{
			Name:         *lun.Name,
			ExternalUUID: *lun.UUID,
		},
		SerialNumber: *lun.SerialNumber,
	}, nil
}

// LunGet retrieves a LUN by calling the ONTAP REST Client
func (rc *OntapRestProvider) LunGet(params LunGetParams) (*LunResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	lun, err := client.SAN().LunGet(&ontapRest.LunGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"status.*", "serial_number", "class", "space.size", "location.*"},
		},
		SvmName:    &params.SvmName,
		VolumeName: &params.VolumeName,
		LunName:    &params.LunName,
	})

	if err != nil {
		return nil, err
	}
	if lun == nil {
		return nil, fmt.Errorf("lun not found: svm=%s, volume=%s, lun=%s", params.SvmName, params.VolumeName, params.LunName)
	}
	return &LunResponse{
		ProviderResponse: ProviderResponse{
			Name:         *lun.Name,
			ExternalUUID: *lun.UUID,
		},
		SerialNumber: *lun.SerialNumber,
	}, nil
}

// IgroupCreate creates an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupCreate(params IgroupCreateParams) (string, error) {
	client := getOntapClientFunc(rc.ClientParams)
	iGroupName, err := client.SAN().IGroupCreate(&ontapRest.IgroupCreateParams{
		Name:       params.IgroupName,
		SvmName:    params.SvmName,
		OsType:     params.OsType,
		Initiators: params.Initiator,
	})
	if err != nil {
		return "", err
	}
	return iGroupName, nil
}

// LunMapCreate creates a LUN mapping by calling the ONTAP REST Client
func (rc *OntapRestProvider) LunMapCreate(params LunMapCreateParams) error {
	for i := 0; i < len(params.IGroupName); i++ {
		client := getOntapClientFunc(rc.ClientParams)
		if err := client.SAN().LunMapCreate(&ontapRest.LunMapCreateParams{
			LunName:    params.LunName,
			SvmName:    params.SvmName,
			IGroupName: params.IGroupName[i],
		}); err != nil {
			if strings.Contains(err.Error(), "LUN already mapped to this group") {
				return errors.NewConflictErr(fmt.Sprintf("LUN %s is already mapped to initiator group %s", params.LunName, params.IGroupName[i]))
			}
			return err
		}
	}
	return nil
}

// IgroupGet creates an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupGet(name, svmName string) (*ontapRest.Igroup, error) {
	client := getOntapClientFunc(rc.ClientParams)
	iGroup, err := client.SAN().IGroupGet(&ontapRest.IgroupGetParams{
		Name:    &name,
		SvmName: svmName,
	})
	if err != nil {
		return nil, err
	}
	return iGroup, nil
}

func (rc *OntapRestProvider) IgroupExists(name, svm string) (bool, error) {
	res, err := rc.IgroupGet(name, svm)
	if err != nil {
		if errors.IsNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}
	if res == nil {
		return false, nil
	}

	return true, nil
}

// IscsiServiceCreate creates an iSCSI service by calling the ONTAP REST Client
func (rc *OntapRestProvider) IscsiServiceCreate(svmUUID string) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.SAN().IscsiServiceCreate(&ontapRest.IscsiCreateParams{
		SvmUUID: svmUUID})
	if err != nil {
		return err
	}
	return nil
}
