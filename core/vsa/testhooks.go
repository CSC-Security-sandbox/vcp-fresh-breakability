package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// TestHooks contains optional overrides for internal helpers used by ONTAP provider logic.
// These hooks are intended for use in unit tests only and are not safe for concurrent use.
type TestHooks struct {
	GetOntapClient                 func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error)
	EnsureCifsServerNamePostFix    func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string) error
	CreateAndSetupCIFSServer       func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string, string) (string, error)
	IsDDNSEnabled                  func(log.Logger, ontapRest.RESTClient, string) bool
	CreateJunctionPathForCifsShare func(ontapRest.RESTClient, string, string) error
	DdnsModify                     func(ontapRest.RESTClient, string, string) error
}

type testHookSnapshot struct {
	getClient                      func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error)
	ensureCifsServerNamePostFix    func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string) error
	createAndSetupCIFSServer       func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string, string) (string, error)
	isDDNSEnabled                  func(log.Logger, ontapRest.RESTClient, string) bool
	createJunctionPathForCifsShare func(ontapRest.RESTClient, string, string) error
	ddnsModify                     func(ontapRest.RESTClient, string, string) error
}

// SetTestHooks applies the provided overrides and returns a cleanup function that restores the previous values.
func SetTestHooks(hooks TestHooks) func() {
	snapshot := testHookSnapshot{
		getClient:                      getOntapClientFunc,
		ensureCifsServerNamePostFix:    ensureCifsServerNamePostFix,
		createAndSetupCIFSServer:       createAndSetupCIFSServer,
		isDDNSEnabled:                  isDDNSEnabled,
		createJunctionPathForCifsShare: createJunctionPathForCifsShare,
		ddnsModify:                     ddnsModify,
	}

	if hooks.GetOntapClient != nil {
		getOntapClientFunc = hooks.GetOntapClient
	}
	if hooks.EnsureCifsServerNamePostFix != nil {
		ensureCifsServerNamePostFix = hooks.EnsureCifsServerNamePostFix
	}
	if hooks.CreateAndSetupCIFSServer != nil {
		createAndSetupCIFSServer = hooks.CreateAndSetupCIFSServer
	}
	if hooks.IsDDNSEnabled != nil {
		isDDNSEnabled = hooks.IsDDNSEnabled
	}
	if hooks.CreateJunctionPathForCifsShare != nil {
		createJunctionPathForCifsShare = hooks.CreateJunctionPathForCifsShare
	}
	if hooks.DdnsModify != nil {
		ddnsModify = hooks.DdnsModify
	}

	return func() {
		if hooks.GetOntapClient != nil {
			getOntapClientFunc = snapshot.getClient
		}
		if hooks.EnsureCifsServerNamePostFix != nil {
			ensureCifsServerNamePostFix = snapshot.ensureCifsServerNamePostFix
		}
		if hooks.CreateAndSetupCIFSServer != nil {
			createAndSetupCIFSServer = snapshot.createAndSetupCIFSServer
		}
		if hooks.IsDDNSEnabled != nil {
			isDDNSEnabled = snapshot.isDDNSEnabled
		}
		if hooks.CreateJunctionPathForCifsShare != nil {
			createJunctionPathForCifsShare = snapshot.createJunctionPathForCifsShare
		}
		if hooks.DdnsModify != nil {
			ddnsModify = snapshot.ddnsModify
		}
	}
}
