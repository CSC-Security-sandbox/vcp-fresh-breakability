package datastores

import (
	"os"
	"testing"

	"github.com/labstack/gommon/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"
)

func TestCreatePoolSpanner(t *testing.T) {
	os.Setenv("SPANNER_EMULATOR_HOST", "http://localhost:9010")
	ds := NewSpannerDatastore("sridhar-yalla", "test-instance", "test-database")

	err := ds.CreatePool(api.Pool{Id: "vxdfdsf1", Name: "dfgdfg"})
	if err != nil {
		t.Errorf("Error creating pool: %v", err)
		return
	}
}

func TestGetPoolSpanner(t *testing.T) {
	os.Setenv("SPANNER_EMULATOR_HOST", "http://localhost:9010")
	ds := NewSpannerDatastore("sridhar-yalla", "test-instance", "test-database")

	pool, err := ds.GetPool("vxdfdsf")
	if err != nil {
		t.Errorf("Error creating pool: %v", err)
		return
	}

	log.Infof("Got pool: %+v", pool)
}
