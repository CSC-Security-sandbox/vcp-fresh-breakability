package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

type monkeyMethods interface {
	getOntapClientFunc(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error)
}
