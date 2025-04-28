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
