package datastores

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ Datastore = (*SpannerDatastore)(nil)

type SpannerDatastore struct{}

func (s SpannerDatastore) GetPool(uuid string) (*api.Pool, error) {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) CreatePool(pool api.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) UpdatePool(pool *api.Pool) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) DeletePool(uuid string) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) GetSVM(uuid string) (*api.SVM, error) {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) CreateSVM(svm *api.SVM) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) UpdateSVM(svm *api.SVM) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) DeleteSVM(uuid string) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) GetVolume(uuid string) (*api.Volume, error) {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) CreateVolume(volume *api.Volume) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) UpdateVolume(volume *api.Volume) error {
	//TODO implement me
	panic("implement me")
}

func (s SpannerDatastore) DeleteVolume(uuid string) error {
	//TODO implement me
	panic("implement me")
}
