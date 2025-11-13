package vsa

import (
	"errors"
	"testing"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSetTestHooks(t *testing.T) {
	// Save original values
	originalGetClient := getOntapClientFunc
	originalEnsureCifsServerNamePostFix := ensureCifsServerNamePostFix
	originalCreateAndSetupCIFSServer := createAndSetupCIFSServer
	originalIsDDNSEnabled := isDDNSEnabled
	originalCreateJunctionPathForCifsShare := createJunctionPathForCifsShare
	originalDdnsModify := ddnsModify

	// Restore original values after test
	defer func() {
		getOntapClientFunc = originalGetClient
		ensureCifsServerNamePostFix = originalEnsureCifsServerNamePostFix
		createAndSetupCIFSServer = originalCreateAndSetupCIFSServer
		isDDNSEnabled = originalIsDDNSEnabled
		createJunctionPathForCifsShare = originalCreateJunctionPathForCifsShare
		ddnsModify = originalDdnsModify
	}()

	t.Run("WhenAllHooksSet_ThenAllFunctionsReplaced", func(tt *testing.T) {
		calledGetClient := false
		calledEnsureCifsServerNamePostFix := false
		calledCreateAndSetupCIFSServer := false
		calledIsDDNSEnabled := false
		calledCreateJunctionPathForCifsShare := false
		calledDdnsModify := false

		mockGetClient := func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			calledGetClient = true
			return nil, nil
		}
		mockEnsureCifsServerNamePostFix := func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string) error {
			calledEnsureCifsServerNamePostFix = true
			return nil
		}
		mockCreateAndSetupCIFSServer := func(log.Logger, ontapRest.RESTClient, *ActiveDirectory, string, string) (string, error) {
			calledCreateAndSetupCIFSServer = true
			return "", nil
		}
		mockIsDDNSEnabled := func(log.Logger, ontapRest.RESTClient, string) bool {
			calledIsDDNSEnabled = true
			return true
		}
		mockCreateJunctionPathForCifsShare := func(ontapRest.RESTClient, string, string) error {
			calledCreateJunctionPathForCifsShare = true
			return nil
		}
		mockDdnsModify := func(ontapRest.RESTClient, string, string) error {
			calledDdnsModify = true
			return nil
		}

		cleanup := SetTestHooks(TestHooks{
			GetOntapClient:                 mockGetClient,
			EnsureCifsServerNamePostFix:    mockEnsureCifsServerNamePostFix,
			CreateAndSetupCIFSServer:       mockCreateAndSetupCIFSServer,
			IsDDNSEnabled:                  mockIsDDNSEnabled,
			CreateJunctionPathForCifsShare: mockCreateJunctionPathForCifsShare,
			DdnsModify:                     mockDdnsModify,
		})

		// Verify hooks are set by calling them
		_, _ = getOntapClientFunc(ontapRest.RESTClientParams{})
		assert.True(tt, calledGetClient)

		_ = ensureCifsServerNamePostFix(nil, nil, nil, "")
		assert.True(tt, calledEnsureCifsServerNamePostFix)

		_, _ = createAndSetupCIFSServer(nil, nil, nil, "", "")
		assert.True(tt, calledCreateAndSetupCIFSServer)

		_ = isDDNSEnabled(nil, nil, "")
		assert.True(tt, calledIsDDNSEnabled)

		_ = createJunctionPathForCifsShare(nil, "", "")
		assert.True(tt, calledCreateJunctionPathForCifsShare)

		_ = ddnsModify(nil, "", "")
		assert.True(tt, calledDdnsModify)

		// Call cleanup
		cleanup()

		// Verify cleanup doesn't panic when called multiple times
		assert.NotPanics(tt, func() {
			cleanup()
		})
	})

	t.Run("WhenPartialHooksSet_ThenOnlySetFunctionsReplaced", func(tt *testing.T) {
		calledGetClient := false
		mockGetClient := func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			calledGetClient = true
			return nil, errors.New("mock error")
		}

		cleanup := SetTestHooks(TestHooks{
			GetOntapClient: mockGetClient,
		})

		// Verify GetOntapClient is set
		_, _ = getOntapClientFunc(ontapRest.RESTClientParams{})
		assert.True(tt, calledGetClient)

		// Call cleanup
		cleanup()

		// Verify cleanup doesn't panic
		assert.NotPanics(tt, func() {
			cleanup()
		})
	})

	t.Run("WhenEmptyHooksSet_ThenNoFunctionsReplaced", func(tt *testing.T) {
		cleanup := SetTestHooks(TestHooks{})

		// Verify functions are still callable (not nil)
		assert.NotNil(tt, getOntapClientFunc)
		assert.NotNil(tt, ensureCifsServerNamePostFix)
		assert.NotNil(tt, createAndSetupCIFSServer)
		assert.NotNil(tt, isDDNSEnabled)
		assert.NotNil(tt, createJunctionPathForCifsShare)
		assert.NotNil(tt, ddnsModify)

		// Call cleanup (should be safe even with no hooks set)
		cleanup()

		// Verify functions are still callable after cleanup
		assert.NotNil(tt, getOntapClientFunc)
		assert.NotNil(tt, ensureCifsServerNamePostFix)
		assert.NotNil(tt, createAndSetupCIFSServer)
		assert.NotNil(tt, isDDNSEnabled)
		assert.NotNil(tt, createJunctionPathForCifsShare)
		assert.NotNil(tt, ddnsModify)
	})

	t.Run("WhenCleanupCalledMultipleTimes_ThenSafe", func(tt *testing.T) {
		mockGetClient := func(ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, nil
		}

		cleanup := SetTestHooks(TestHooks{
			GetOntapClient: mockGetClient,
		})

		// First cleanup
		cleanup()

		// Second cleanup should be safe
		assert.NotPanics(tt, func() {
			cleanup()
		})
	})
}

