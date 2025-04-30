package vsa

import (
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func (rc *OntapRestProvider) IsAggregateOnline(aggregateName string) (bool, error) {
	aggr, err := rc.client.Storage().AggregateFindByName(&ontapRest.AggregateCollectionGetParams{
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
	aggr, err := rc.client.Storage().AggregateFindByName(&ontapRest.AggregateCollectionGetParams{
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
func (rc *OntapRestProvider) LunCreate(params LunCreateParams) (*ProviderResponse, error) {
	lun, err := rc.client.SAN().LunCreate(&ontapRest.LunCreateParams{
		Name:       params.LunName,
		SvmName:    params.SvmName,
		OsType:     params.OsType,
		VolumeName: params.VolumeName,
		Size:       params.Size,
	})
	if err != nil {
		return nil, err
	}
	return &ProviderResponse{
		Name:         *lun.Name,
		ExternalUUID: *lun.UUID,
	}, nil
}

// IgroupCreate creates an initiator group by calling the ONTAP REST Client
func (rc *OntapRestProvider) IgroupCreate(params IgroupCreateParams) (string, error) {
	iGroupName, err := rc.client.SAN().IGroupCreate(&ontapRest.IgroupCreateParams{
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
		if err := rc.client.SAN().LunMapCreate(&ontapRest.LunMapCreateParams{
			LunName:    params.LunName,
			SvmName:    params.SvmName,
			IGroupName: params.IGroupName[i],
			LunNumber:  &params.LunNumber,
		}); err != nil {
			return err
		}
	}
	return nil
}
