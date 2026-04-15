package ociserver

import (
	"math"
	"testing"

	"github.com/ogen-go/ogen/validate"
	"github.com/stretchr/testify/require"
)

func TestCreatePoolRequest_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*CreatePoolRequest)(nil).Validate(), validate.ErrNilPointer)

	req := CreatePoolRequest{ThroughputGBps: 1}
	require.NoError(t, req.Validate())

	bad := CreatePoolRequest{ThroughputGBps: math.NaN()}
	require.Error(t, bad.Validate())
}

func TestError_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*Error)(nil).Validate(), validate.ErrNilPointer)

	e := Error{Code: 500}
	require.NoError(t, e.Validate())

	bad := Error{Code: math.NaN()}
	require.Error(t, bad.Validate())
}

func TestErrorStatusCode_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*ErrorStatusCode)(nil).Validate(), validate.ErrNilPointer)

	esc := ErrorStatusCode{
		Response: Error{Code: 400},
	}
	require.NoError(t, esc.Validate())

	bad := ErrorStatusCode{
		Response: Error{Code: math.NaN()},
	}
	require.Error(t, bad.Validate())
}

func TestGetWorkflowStatusResponse_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*GetWorkflowStatusResponse)(nil).Validate(), validate.ErrNilPointer)

	g := GetWorkflowStatusResponse{}
	require.NoError(t, g.Validate())

	meta := OCICreatePoolWorkflowMetadata{
		InterclusterIPs: []string{"10.0.0.1"},
		NodeIPs:         []string{"10.0.0.2"},
	}
	g2 := GetWorkflowStatusResponse{}
	g2.Metadata.SetTo(meta)
	require.NoError(t, g2.Validate())

	badMeta := OCICreatePoolWorkflowMetadata{}
	g3 := GetWorkflowStatusResponse{}
	g3.Metadata.SetTo(badMeta)
	require.Error(t, g3.Validate())
}

func TestGetWorkflowStatusResponseHeaders_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*GetWorkflowStatusResponseHeaders)(nil).Validate(), validate.ErrNilPointer)

	h := GetWorkflowStatusResponseHeaders{
		Response: GetWorkflowStatusResponse{},
	}
	require.NoError(t, h.Validate())
}

func TestOCICreatePoolWorkflowMetadata_Validate(t *testing.T) {
	t.Parallel()
	require.ErrorIs(t, (*OCICreatePoolWorkflowMetadata)(nil).Validate(), validate.ErrNilPointer)

	m := OCICreatePoolWorkflowMetadata{
		InterclusterIPs: []string{"a"},
		NodeIPs:         []string{"b"},
	}
	require.NoError(t, m.Validate())

	nilIPs := OCICreatePoolWorkflowMetadata{
		InterclusterIPs: nil,
		NodeIPs:         []string{"b"},
	}
	require.Error(t, nilIPs.Validate())
}

func TestStandardErrorHeaders_Validate(t *testing.T) {
	t.Parallel()
	t.Run("400", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError400Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError400Headers{Response: Error{Code: 400}}
		require.NoError(t, h.Validate())
	})
	t.Run("401", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError401Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError401Headers{Response: Error{Code: 401}}
		require.NoError(t, h.Validate())
	})
	t.Run("403", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError403Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError403Headers{Response: Error{Code: 403}}
		require.NoError(t, h.Validate())
	})
	t.Run("404", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError404Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError404Headers{Response: Error{Code: 404}}
		require.NoError(t, h.Validate())
	})
	t.Run("429", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError429Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError429Headers{Response: Error{Code: 429}}
		require.NoError(t, h.Validate())
	})
	t.Run("500", func(t *testing.T) {
		require.ErrorIs(t, (*StandardError500Headers)(nil).Validate(), validate.ErrNilPointer)
		h := StandardError500Headers{Response: Error{Code: 500}}
		require.NoError(t, h.Validate())
	})
}
