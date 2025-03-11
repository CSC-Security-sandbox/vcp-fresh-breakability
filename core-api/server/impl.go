package server

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/datastores"
)

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ api.ServerInterface = (*Server)(nil)

type Server struct {
	d datastores.Datastore
}

func (s Server) GetPoolsPoolId(ctx echo.Context, poolId string) error {
	//TODO implement me
	panic("implement me")
}

func New(ds datastores.Datastore) *Server {
	return &Server{d: ds}
}

func (s Server) PutPoolsPoolId(ctx echo.Context, poolId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetSnapshots(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PostSnapshots(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) DeleteSnapshotsSnapshotId(ctx echo.Context, snapshotId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetSnapshotsSnapshotId(ctx echo.Context, snapshotId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PutSnapshotsSnapshotId(ctx echo.Context, snapshotId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetSvms(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PostSvms(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) DeleteSvmsSvmId(ctx echo.Context, svmId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetSvmsSvmId(ctx echo.Context, svmId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PutSvmsSvmId(ctx echo.Context, svmId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetVolumes(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PostVolumes(ctx echo.Context) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) DeleteVolumesVolumeId(ctx echo.Context, volumeId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) GetVolumesVolumeId(ctx echo.Context, volumeId string) error {
	//TODO implement me
	panic("implement me")
}

func (s Server) PutVolumesVolumeId(ctx echo.Context, volumeId string) error {
	//TODO implement me
	panic("implement me")
}

// GetV1Pools implements the /v1/pools endpoint
func (s Server) GetV1Pools(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, common.Helper())
}

func NewServer() Server {
	return Server{}
}
