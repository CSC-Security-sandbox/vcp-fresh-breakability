package datastores

import (
	"testing"

	"github.com/labstack/gommon/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"
)

func TestCreatePool(t *testing.T) {
	ds := NewFireStoreDatastore("sridhar-yalla", "test-vsa")

	err := ds.CreatePool(api.Pool{Id: "vxdfdsf", Name: "dfgdfg"})
	if err != nil {
		t.Errorf("Error creating pool: %v", err)
		return
	}
}

func TestGetPool(t *testing.T) {
	ds := NewFireStoreDatastore("sridhar-yalla", "test-vsa")

	pool, err := ds.GetPool("vxdfdsf")
	if err != nil {
		t.Errorf("Error creating pool: %v", err)
		return
	}

	log.Infof("Got pool: %+v", pool)
}
