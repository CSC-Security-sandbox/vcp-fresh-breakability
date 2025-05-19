package ontap_rest

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ottransport "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestPaginate(t *testing.T) {
	t.Run("WhenPaginationFuncReturnsError_ThenReturnError", func(tt *testing.T) {
		mockPaginationFunc := func(next string) ([]*models.Job, string, error) {
			return nil, "", errors.New("pagination error")
		}
		mockCallbackFunc := func(payload []*models.Job) error {
			return nil
		}

		err := _paginate(mockPaginationFunc, mockCallbackFunc)
		assert.EqualError(tt, err, "pagination error")
	})

	t.Run("WhenCallbackFuncReturnsError_ThenReturnError", func(tt *testing.T) {
		mockPaginationFunc := func(next string) ([]*models.Job, string, error) {
			return []*models.Job{{}}, "", nil
		}
		mockCallbackFunc := func(payload []*models.Job) error {
			return errors.New("callback error")
		}

		err := _paginate(mockPaginationFunc, mockCallbackFunc)
		assert.EqualError(tt, err, "callback error")
	})

	t.Run("WhenPaginationSucceeds_ThenProcessAllPages", func(tt *testing.T) {
		mockPaginationFunc := func(next string) ([]*models.Job, string, error) {
			if next == "" {
				return []*models.Job{{}}, "nextPage", nil
			}
			return []*models.Job{{}}, "", nil
		}
		var processedJobs int
		mockCallbackFunc := func(payload []*models.Job) error {
			processedJobs += len(payload)
			return nil
		}

		err := _paginate(mockPaginationFunc, mockCallbackFunc)
		assert.NoError(tt, err)
		assert.Equal(tt, 2, processedJobs) // Two pages processed
	})
}

func TestSetNext(t *testing.T) {
	t.Run("WhenContextIsNil_ThenCreateNewContext", func(tt *testing.T) {
		next := "nextPage"
		ctx := setNext(context.Background(), next)
		assert.NotNil(tt, ctx)
		assert.Equal(tt, next, ctx.Value(ottransport.NextContextKey))
	})

	t.Run("WhenContextIsNotNil_ThenAddNextValue", func(tt *testing.T) {
		next := "nextPage"
		type contextKey string
		const myContextKey contextKey = "key"
		contextValue := "value"
		ctx := context.WithValue(context.Background(), myContextKey, contextValue)
		ctx = setNext(ctx, next)
		assert.NotNil(tt, ctx)
		assert.Equal(tt, next, ctx.Value(ottransport.NextContextKey))
	})
}

func TestGetConstrainedMaxRecords(t *testing.T) {
	t.Run("WhenMaxRecordsIsNil_ThenReturnDefaultPageSize", func(tt *testing.T) {
		result := getConstrainedMaxRecords(nil)
		assert.NotNil(tt, result)
		assert.Equal(tt, "10000", *result)
	})

	t.Run("WhenMaxRecordsExceedsDefault_ThenReturnDefaultPageSize", func(tt *testing.T) {
		maxRecords := nillable.ToPointer(int64(20000))
		result := getConstrainedMaxRecords(maxRecords)
		assert.NotNil(tt, result)
		assert.Equal(tt, "10000", *result)
	})

	t.Run("WhenMaxRecordsIsWithinLimit_ThenReturnMaxRecords", func(tt *testing.T) {
		maxRecords := nillable.ToPointer(int64(5000))
		result := getConstrainedMaxRecords(maxRecords)
		assert.NotNil(tt, result)
		assert.Equal(tt, "5000", *result)
	})
}
