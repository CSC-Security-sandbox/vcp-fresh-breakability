package ontap_rest

import (
	"context"
	"fmt"
	"strconv"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ottransport "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// MD: for any new type that will use the pagination engine it should be added here as a constraint, for type safety.
type pageable interface {
	[]*models.Job | []*Schedule | []*models.BroadcastDomain | []*IPInterface | []*IPServicePolicy |
		[]*models.NetworkRoute | []*Aggregate | []*models.Disk | []*Snapshot | []*models.SnapshotPolicy |
		[]*Volume | []*Job | []*Svm | []*Node | []*BroadcastDomain |
		[]*SvmPeer | []*QosPolicy | []string | []*Igroup | []*Lun | []*ExportPolicy | []*CifsGroup
}

type ontapPaginationFunc[T pageable] func(next string) (T, string, error)

// UserCallbackFunc is the callback function defined by the API user. The engine calls this function with results from Ontap until no more exist.
type UserCallbackFunc[T pageable] func(payload T) error

func _paginate[T pageable](opf ontapPaginationFunc[T], ucbf UserCallbackFunc[T]) error {
	var response T
	var next string
	var err error

	for while := true; while; while = next != "" {
		response, next, err = opf(next)
		if err != nil {
			return err
		}

		if err = ucbf(response); err != nil {
			return err
		}

		// MD: should we add some sleep here for request throttling?
	}

	return nil
}

func setNext(ctx context.Context, next string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, ottransport.NextContextKey, next)
}

var defaultOntapPageSize = nillable.ToPointer(int64(env.GetUint("ONTAP_MAX_PAGINATED_RECORDS", 10000)))

func getConstrainedMaxRecords(maxRecords *int64) *string {
	mr := strconv.FormatInt(*defaultOntapPageSize, 10)
	if maxRecords == nil || *maxRecords > *defaultOntapPageSize {
		return &mr
	}
	mr = fmt.Sprintf("%d", *maxRecords)
	return &mr
}
