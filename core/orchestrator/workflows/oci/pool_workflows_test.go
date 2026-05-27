package oci

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	vmrs_oci "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/oci"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	envs "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	testVSAImageOCID      = "ocid1.image.oc1.iad.aaaaaaaaef2bc4g6vf4rvsa4vd2e4pnqw2ot2qicxrjo5a3ohglr6i4exdjq"
	testMediatorImageOCID = "ocid1.image.oc1.iad.aaaaaaaagakcrtyceuuvl6ts7xhqzzrdk3lv4z7tcqif3xpa6qsvppzflaaq"
	testOCIOntapVersion   = "9.18.1"
)

// withVSAImageOCIDs sets package-level image OCIDs for the test (init-time env is not re-read; tests must assign).
func withVSAImageOCIDs(t *testing.T, vsa, mediator string) {
	t.Helper()
	origV, origM := vsaImageName, vsaMediatorImageName
	vsaImageName, vsaMediatorImageName = vsa, mediator
	t.Cleanup(func() {
		vsaImageName, vsaMediatorImageName = origV, origM
	})
}

// withOCIOntapVersionDetails pins the package-level ONTAP version source that
// utils.GetOntapVersionBasedOnAllowlisting reads from, and clears the
// experimental override so non-allowlisted accounts deterministically receive
// `current` regardless of the test's account name. Originals are restored via
// t.Cleanup.
func withOCIOntapVersionDetails(t *testing.T, current string) {
	t.Helper()
	origCurrent := envs.CurrentOntapVersionDetails
	origExperimental := envs.ExperimentalOntapVersionDetails
	envs.CurrentOntapVersionDetails = current
	envs.ExperimentalOntapVersionDetails = ""
	t.Cleanup(func() {
		envs.CurrentOntapVersionDetails = origCurrent
		envs.ExperimentalOntapVersionDetails = origExperimental
	})
}

func setTestOCIImageEnv(t *testing.T) {
	t.Helper()
	withVSAImageOCIDs(t, testVSAImageOCID, testMediatorImageOCID)
	withOCIOntapVersionDetails(t, testOCIOntapVersion)
}

// setOCIExpertModePassword overrides the package-level ociExpertModePassword
// so the workflow skips the GetExpertModeCredentialsForOCI activity and
// uses the preset password directly. The original value is restored via t.Cleanup.
func setOCIExpertModePassword(t *testing.T, pw string) {
	t.Helper()
	orig := ociExpertModePassword
	ociExpertModePassword = pw
	t.Cleanup(func() { ociExpertModePassword = orig })
}

// withOCIVMRSEnabled overrides the package-level ociVMRSEnabled toggle so a
// test can drive OCICreatePoolWorkflow's VMRS branch (workflow.SideEffect
// captures the toggle at start; this helper flips the value the SideEffect
// observes). Original value is restored via t.Cleanup.
func withOCIVMRSEnabled(t *testing.T, enabled bool) {
	t.Helper()
	orig := ociVMRSEnabled
	ociVMRSEnabled = enabled
	t.Cleanup(func() { ociVMRSEnabled = orig })
}

// registerOCICreatePoolVLMRollbackWorkflows registers the VLM delete child workflow used when OCICreatePoolWorkflow
// rolls back after CreateVSAClusterDeployment (or later steps) fail.
func registerOCICreatePoolVLMRollbackWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *vlm.DeleteVSAClusterDeploymentRequest, _ string) error {
			return nil
		},
		workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
	)
}

func withOCIWorkerStartupEnv(t *testing.T, vsa, mediator, adminPassword, region, secret string) {
	t.Helper()
	origVsa, origMediator := vsaImageName, vsaMediatorImageName
	origAdminPassword, origRegion, origSecret := ociOntapAdminPassword, localRegion, secretURI
	vsaImageName = vsa
	vsaMediatorImageName = mediator
	ociOntapAdminPassword = adminPassword
	localRegion = region
	secretURI = secret
	t.Cleanup(func() {
		vsaImageName, vsaMediatorImageName = origVsa, origMediator
		ociOntapAdminPassword, localRegion, secretURI = origAdminPassword, origRegion, origSecret
	})
}

// registerOCICreatePoolOntapCredentialMocks stubs OCI Vault admin credential activities so tests do
// not call real OCI APIs. The OCI create-pool workflow always invokes CreateOnTapCredentialsForOCI
// (the activity itself decides whether to provision a vault secret based on pool.PoolCredentials.AuthType).
// Call only after env.RegisterActivity(&activities.PoolActivity{...}) (Temporal test suite requirement).
func registerOCICreatePoolOntapCredentialMocks(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity("CreateOnTapCredentialsForOCI", mock.Anything, mock.Anything).
		Return(&activities.OCICreatePoolCredentials{
			OntapCredentials: vlm.OntapCredentials{AdminPassword: "test-ontap-admin-password"},
			Secret:           &datamodel.ExternalCredRef{Name: "test-secret", Version: 1, ExternalIdentifier: "ocid1.vaultsecret.oc1..testsecretocid"},
		}, nil).Maybe()
	env.OnActivity("DeleteOnTapCredentialsForOCI", mock.Anything, mock.Anything).Return(nil).Maybe()
}

func TestValidateOCIWorkerStartupEnv(t *testing.T) {
	t.Run("ok when all required vars are present", func(t *testing.T) {
		withOCIWorkerStartupEnv(t, testVSAImageOCID, testMediatorImageOCID, "Netapp1!", "us-ashburn-1", "ocid1.vaultsecret.oc1..aaa")
		assert.NoError(t, ValidateOCIWorkerStartupEnv())
	})

	t.Run("fails and lists missing vars", func(t *testing.T) {
		withOCIWorkerStartupEnv(t, "", "", "", "", "")
		err := ValidateOCIWorkerStartupEnv()
		assert.Error(t, err)
		assert.True(t, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(t, err.Error(), "VSA_IMAGE_NAME")
		assert.Contains(t, err.Error(), "VSA_MEDIATOR_IMAGE_NAME")
		assert.Contains(t, err.Error(), "OCI_ONTAP_ADMIN_PASSWORD")
		assert.Contains(t, err.Error(), "LOCAL_REGION")
		assert.Contains(t, err.Error(), "SECRET_URI")
	})
}

func TestPrepareVLMConfig_CustomPerformanceAndHardcodedSerialPrefix(t *testing.T) {
	setTestOCIImageEnv(t)
	iops := int64(5000)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}
	cfg, err := prepareVLMConfig(params, pool, nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, int64(128), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(5000), cfg.Deployment.SPConfig.IOps)

	assert.Equal(t, ociSerialNumberLeadingPrefix+ociSerialNumberPrefix, cfg.Deployment.SerialNumberPrefix,
		"SerialNumberPrefix must be the hardcoded \"955\"+15 zeros emitted by the workflow; "+
			"the API field is currently ignored end-to-end and the workflow is the single source of truth")
}

func TestPrepareVLMConfig_DerivesIopsFromThroughputWhenNil(t *testing.T) {
	setTestOCIImageEnv(t)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            nil, // derived by the validator
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}

	cfg, err := prepareVLMConfig(params, pool, nil)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	const expectedDerivedIOPS = int64(2048)
	assert.Equal(t, int64(128), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, expectedDerivedIOPS, cfg.Deployment.SPConfig.IOps,
		"SPConfig.IOps must equal the validator-derived IOPS (ThroughputMibps * IopsPerMiBps)")
}

func TestPrepareVLMConfig_ReturnsErrorWhenIopsValidationFails(t *testing.T) {
	setTestOCIImageEnv(t)
	belowMin := int64(100) // below MinCustomIops (1024)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &belowMin,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}

	cfg, err := prepareVLMConfig(params, pool, nil)
	assert.Error(t, err, "validator must reject IOPS below MinCustomIops")
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "derive iops from throughput")
}

func TestPrepareVLMConfig_RejectsZeroHAPairs(t *testing.T) {
	setTestOCIImageEnv(t)
	iops := int64(5000)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		// HAPairs intentionally omitted (zero value).
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}

	cfg, err := prepareVLMConfig(params, pool, nil)
	require.Error(t, err, "zero HAPairs must be rejected")
	assert.Nil(t, cfg)
	assert.True(t, utilserrors.IsUserInputValidationErr(err),
		"error must be a UserInputValidationErr so it surfaces as a 4xx, not a 5xx")
	assert.Contains(t, err.Error(), "haPairs",
		"error message must mention the offending field")
}

// TestComputeOCIVMRSInput_RejectsMissingOrZeroThroughput is the
// regression guard for the nil-deref panic that existed when
// computeOCIVMRSInput read params.CustomPerformanceParams.ThroughputMibps
// without a nil check. The headline case is the nil sub-test: previously
// it crashed the workflow worker (Temporal would mark the activity as
// non-deterministic and the workflow would get stuck); now it must
// return a user-input validation error so the API surfaces a 4xx.
// The zero-throughput sub-test is the sibling: a present-but-empty
// CustomPerformanceParams would have sailed through here and tripped
// Select's "DesiredThroughputGBs must be > 0" deeper in the stack with
// a less-actionable error.
func TestComputeOCIVMRSInput_RejectsMissingOrZeroThroughput(t *testing.T) {
	// Pin dataDiskCount so this test doesn't depend on
	// OCI_VSA_DATA_DISK_COUNT in the test runner's environment.
	origDDC := dataDiskCount
	dataDiskCount = 2
	t.Cleanup(func() { dataDiskCount = origDDC })

	base := func() *common.CreatePoolParams {
		return &common.CreatePoolParams{
			HAPairs:     2,
			SizeInBytes: 4 * 1_000_000_000_000, // 4 decimal TB
		}
	}

	cases := []struct {
		name           string
		mutate         func(*common.CreatePoolParams)
		wantMsgInclude string
	}{
		{
			name: "NilCustomPerformanceParams",
			mutate: func(p *common.CreatePoolParams) {
				p.CustomPerformanceParams = nil
			},
			wantMsgInclude: "customPerformanceParams is required",
		},
		{
			name: "ZeroThroughputMibps",
			mutate: func(p *common.CreatePoolParams) {
				p.CustomPerformanceParams = &common.CustomPerformanceParams{ThroughputMibps: 0}
			},
			wantMsgInclude: "throughputMibps must be > 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			params := base()
			tc.mutate(params)

			perVMCap, perVMThru, err := computeOCIVMRSInput(params)

			require.Error(tt, err, "must reject before dereferencing throughput")
			assert.Zero(tt, perVMCap)
			assert.Zero(tt, perVMThru)
			assert.True(tt, utilserrors.IsUserInputValidationErr(err),
				"error must be a UserInputValidationErr so it surfaces as a 4xx, not a panic")
			assert.Contains(tt, err.Error(), tc.wantMsgInclude,
				"error message must point at the offending field")
		})
	}
}

// TestComputeOCIVMRSInput_RejectsTopologyInputs covers the
// shape/size/data-disk validators that fire BEFORE
// CustomPerformanceParams is read. Each sub-test exercises exactly one
// invalid input; the others stay valid so the failing field is the
// only candidate, anchoring both the precondition order and the error
// message that surfaces (so 4xx responses stay actionable).
func TestComputeOCIVMRSInput_RejectsTopologyInputs(t *testing.T) {
	origDDC := dataDiskCount
	t.Cleanup(func() { dataDiskCount = origDDC })

	validCPP := func() *common.CustomPerformanceParams {
		return &common.CustomPerformanceParams{ThroughputMibps: 1024}
	}

	cases := []struct {
		name           string
		setup          func()
		params         *common.CreatePoolParams
		wantMsgInclude string
	}{
		{
			name:  "ZeroHAPairs",
			setup: func() { dataDiskCount = 2 },
			params: &common.CreatePoolParams{
				HAPairs:                 0, // <- offending field
				SizeInBytes:             4 * 1_000_000_000_000,
				CustomPerformanceParams: validCPP(),
			},
			wantMsgInclude: "haPairs must be > 0",
		},
		{
			name:  "ZeroDataDiskCount",
			setup: func() { dataDiskCount = 0 }, // <- offending field
			params: &common.CreatePoolParams{
				HAPairs:                 2,
				SizeInBytes:             4 * 1_000_000_000_000,
				CustomPerformanceParams: validCPP(),
			},
			wantMsgInclude: "OCI_VSA_DATA_DISK_COUNT must be > 0",
		},
		{
			name:  "ZeroSizeInBytes",
			setup: func() { dataDiskCount = 2 },
			params: &common.CreatePoolParams{
				HAPairs:                 2,
				SizeInBytes:             0, // <- offending field
				CustomPerformanceParams: validCPP(),
			},
			wantMsgInclude: "SizeInBytes must be > 0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			tc.setup()
			perVMCap, perVMThru, err := computeOCIVMRSInput(tc.params)
			require.Error(tt, err)
			assert.Zero(tt, perVMCap)
			assert.Zero(tt, perVMThru)
			assert.True(tt, utilserrors.IsUserInputValidationErr(err),
				"all topology validators must surface as UserInputValidationErr (4xx)")
			assert.Contains(tt, err.Error(), tc.wantMsgInclude)
		})
	}
}

// TestComputeOCIVMRSInput_ActivePassiveSplit anchors the AP-mode
// branch: when IsRegionalHA=true (non-shared HA), only the primary VM
// in each pair serves I/O, so totalActiveVMs = HAPairs (not 2 *
// HAPairs) and BOTH per-VM capacity AND per-VM throughput are exactly
// double the AA value for the same total. This sub-test paired with
// the AA happy path guarantees the two modes can't accidentally
// converge during a refactor of the activeVMsPerPair branch — and it
// is the regression guard for the historical bug where the capacity
// formula divided by HAPairs * dataDiskCount instead of
// totalActiveVMs (which only coincidentally produced the correct AA
// number when dataDiskCount==2 and which was 2x off in AP).
func TestComputeOCIVMRSInput_ActivePassiveSplit(t *testing.T) {
	origDDC := dataDiskCount
	dataDiskCount = 2
	t.Cleanup(func() { dataDiskCount = origDDC })

	params := &common.CreatePoolParams{
		HAPairs:      2,
		IsRegionalHA: true, // Active-Passive: only primary serves
		SizeInBytes:  4 * 1_000_000_000_000,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 1024, // ≈ 1.073741824 GB/s
		},
	}

	perVMCap, perVMThru, err := computeOCIVMRSInput(params)
	require.NoError(t, err)
	// AP → totalActiveVMs = HAPairs = 2 → perVM capacity = 4 / 2 = 2.0
	// (exactly 2x the AA case which divides by 4 = totalActiveVMs in AA).
	// Pre-fix this returned 1.0 because the formula used
	// HAPairs * dataDiskCount (= 4) as the denominator regardless of
	// AA/AP — the failure mode this test guards against.
	assert.InDelta(t, 2.0, perVMCap, 1e-9,
		"AP per-VM capacity must be total / HAPairs (not / 2*HAPairs and not / HAPairs*dataDiskCount)")
	// AP → totalActiveVMs = HAPairs = 2 → perVM throughput = 0.536870912
	// (exactly 2x the AA case which divides by 4)
	assert.InDelta(t, 0.536870912, perVMThru, 1e-9,
		"AP per-VM throughput must be total / HAPairs (not / 2*HAPairs)")
}

// TestPrepareVLMConfig_AppliesDecisionFlipsAllFourOCIFields is the
// regression guard for the `if decision != nil` block inside
// prepareVLMConfig — the four-field VMRS override that gets activated
// only when OCI_VMRS_ENABLED=true. Without this test the entire block
// (VMRS catalogue → OCIConfig) is reachable only by an end-to-end
// workflow test, and the existing prepareVLMConfig tests all pass nil
// for the decision argument.
func TestPrepareVLMConfig_AppliesDecisionFlipsAllFourOCIFields(t *testing.T) {
	setTestOCIImageEnv(t)
	// CustomPerformanceParams settings here only need to satisfy
	// validators.ValidateIops — they're orthogonal to the VMRS
	// decision-flip behavior we're asserting. Mirroring the values used
	// in TestPrepareVLMConfig_RejectsZeroHAPairs keeps the IOPS floor
	// happy without coupling this test to the validator's exact math.
	iops := int64(5000)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         2,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}
	// Synthetic VMRS Decision that intentionally uses values DIFFERENT
	// from the env defaults so a regression flipping a single field
	// back to the env path can't pass this test.
	decision := &vmrs_oci.Decision{
		VMShape:   "VM.DenseIO.Custom.Flex",
		OCPUs:     40,
		MemoryGBs: 480,
		VPU:       90,
		IOPS:      786600,
	}

	cfg, err := prepareVLMConfig(params, pool, decision)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	oc := cfg.Deployment.OCIConfig
	assert.Equal(t, "VM.DenseIO.Custom.Flex", oc.VSAInstanceShape,
		"VMRS-chosen shape must land on OCIConfig.VSAInstanceShape")
	assert.Equal(t, float32(40), oc.VSAFlexOcpus,
		"VMRS-chosen OCPUs must land on OCIConfig.VSAFlexOcpus")
	assert.Equal(t, float32(480), oc.VSAFlexMemoryInGBs,
		"VMRS-chosen memory must land on OCIConfig.VSAFlexMemoryInGBs verbatim (no per-OCPU derivation)")
	assert.Equal(t, int64(90), oc.DataDiskVpus,
		"VMRS-chosen VPU must land on OCIConfig.DataDiskVpus")
	// The deployment-level VSAInstanceType is wired off OCIConfig.VSAInstanceShape
	// inside ociDeploymentConfig — if the decision shape doesn't flow
	// there, the VM that actually gets launched will be wrong even
	// though OCIConfig looks right.
	assert.Equal(t, "VM.DenseIO.Custom.Flex", cfg.Deployment.VSAInstanceType,
		"VMRS shape must also flow into Deployment.VSAInstanceType so the launched VM matches the Decision")
}

// TestComputeOCIVMRSInput_HappyPath asserts the topology math on a
// well-formed Active-Active 2-HA-pair request. Anchors the per-VM
// throughput AND per-VM capacity formulas so future refactors of the
// AA/AP split can't quietly change what the OCI VMRS activity sees.
//
// Pair this with TestComputeOCIVMRSInput_ActivePassiveSplit: same
// inputs except for IsRegionalHA, and AP must show exactly 2x the
// per-VM numbers AA does (because AP halves the count of serving VMs).
func TestComputeOCIVMRSInput_HappyPath(t *testing.T) {
	origDDC := dataDiskCount
	dataDiskCount = 2
	t.Cleanup(func() { dataDiskCount = origDDC })

	params := &common.CreatePoolParams{
		HAPairs:      2,
		IsRegionalHA: false, // Active-Active: both VMs in each pair serve
		SizeInBytes:  4 * 1_000_000_000_000,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 1024, // ≈ 1.073741824 GB/s
		},
	}

	perVMCap, perVMThru, err := computeOCIVMRSInput(params)
	require.NoError(t, err)
	// totalCapacityTB = 4.0; AA → 2 active VMs per pair →
	// totalActiveVMs = 4 → perVM capacity = 4.0 / 4 = 1.0 TB.
	assert.InDelta(t, 1.0, perVMCap, 1e-9,
		"AA per-VM capacity = total / (2 * HAPairs)")
	// totalThroughputGBs = 1024 MiB/s * 1.048576 / 1000 = 1.073741824
	// AA → 2 active VMs per pair → totalActiveVMs = 4 → perVM = 0.268435456
	assert.InDelta(t, 0.268435456, perVMThru, 1e-9,
		"AA per-VM throughput = total / (2 * HAPairs)")
}

func TestOCIDeploymentConfig(t *testing.T) {
	setTestOCIImageEnv(t)

	cases := []struct {
		name               string
		isRegionalHA       bool
		wantDeploymentTyp  string
		wantEnableAAConfig bool
	}{
		{
			name:               "RegionalHA_NonShared_AADisabled",
			isRegionalHA:       true,
			wantDeploymentTyp:  vlm.DeploymentTypeNonSharedHA,
			wantEnableAAConfig: false,
		},
		{
			name:               "ZonalHA_Shared_AAEnabled",
			isRegionalHA:       false,
			wantDeploymentTyp:  vlm.DeploymentTypeSharedHA,
			wantEnableAAConfig: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			params := &common.CreatePoolParams{
				AccountName:     "acct",
				PrimaryZone:     "ad1",
				SecondaryZone:   "ad2",
				MediatorZone:    "ad3",
				CompartmentOCID: "comp",
				VendorSubNetID:  "subnet",
				DataNICSubnetID: "data-subnet",
				HAPairs:         2,
				IsRegionalHA:    tc.isRegionalHA,
			}
			pool := &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-deploy"},
				Name:           "pool-name",
				DeploymentName: "dep-deploy",
			}
			ociConfig := vlm.OCIConfig{
				CompartmentID:    params.CompartmentOCID,
				SubnetID:         params.VendorSubNetID,
				DataNICSubnetID:  params.DataNICSubnetID,
				VSAInstanceShape: ociVSAInstanceType,
			}

			got := ociDeploymentConfig(params, pool, "100Gi", 256, 8192, ociConfig)

			// HA-mode-derived behavior: both DeploymentType and EnableAAConfig
			// must follow IsRegionalHA in lockstep. See the doc comment above for
			// why these two are intentionally coupled.
			assert.Equal(tt, tc.wantDeploymentTyp, got.DeploymentType,
				"DeploymentType must be derived from params.IsRegionalHA")
			assert.Equal(tt, tc.wantEnableAAConfig, got.DeploymentConfigFlags.EnableAAConfig,
				"DeploymentConfigFlags.EnableAAConfig must be the inverse of params.IsRegionalHA: AA on iff shared HA")
			assert.False(tt, got.DeploymentConfigFlags.EnableAASupportSvm,
				"EnableAASupportSvm is not wired for OCI; should be false in both HA modes")
			assert.False(tt, got.DeploymentConfigFlags.EnableIlbSupport,
				"EnableIlbSupport is not wired for OCI; should be false in both HA modes")
			assert.Empty(tt, got.DeploymentConfigFlags.EnableNfsV364BitIdentifier,
				"EnableNfsV364BitIdentifier is not wired for OCI; should be empty in both HA modes")

			// Invariant fields - regression guard so the IsRegionalHA refactor cannot
			// silently break the rest of the deployment-config wiring.
			assert.Equal(tt, vlm.OCICloud, got.Provider)
			assert.Equal(tt, pool.DeploymentName, got.DeploymentID)
			assert.Equal(tt, ociSerialNumberLeadingPrefix+ociSerialNumberPrefix, got.SerialNumberPrefix,
				"SerialNumberPrefix must be the hardcoded \"955\"+15 zeros — the workflow owns the format, params.SerialNumberPrefix no longer exists and the API value is ignored")
			assert.Equal(tt, localRegion, got.Region)
			assert.Equal(tt, vsaImageName, got.Images.VSAImageName)
			assert.Equal(tt, vsaMediatorImageName, got.Images.MediatorImageName)
			assert.Equal(tt, ociVSAUserBootargs, got.UserBootargs)
			assert.Equal(tt, pool.Name, got.Labels["pool_name"])
			assert.Equal(tt, pool.UUID, got.Labels["pool_uuid"])
			assert.Equal(tt, params.AccountName, got.Labels["account_id"])
			// pool_ocid is added later by prepareCreateVSAClusterDeploymentRequest;
			// ociDeploymentConfig itself should not set it.
			_, hasPoolOCID := got.Labels["pool_ocid"]
			assert.False(tt, hasPoolOCID, "ociDeploymentConfig must not set pool_ocid label; it is added downstream")
			assert.Equal(tt, int(params.HAPairs), got.NumHAPair)
			assert.Equal(tt, ociVSAInstanceType, got.VSAInstanceType)
			assert.Equal(tt, ociMediatorInstanceType, got.MediatorInstanceType)
			assert.Equal(tt, dataDiskCount, got.DataDiskCount)
			assert.Equal(tt, ociConfig, got.OCIConfig)
			assert.Equal(tt, "100Gi", got.SPConfig.Size)
			assert.Equal(tt, int64(256), got.SPConfig.Throughput)
			assert.Equal(tt, int64(8192), got.SPConfig.IOps)
			assert.Equal(tt, extIPForNodeMgmt, got.DevFlags.ExtIPForNodeMgmt)
			assert.Equal(tt, allowNonDenseShapeForVSA, got.DevFlags.AllowNonDenseShapeForVsa)
		})
	}
}

// TestPrepareVLMConfig_DeploymentTypeReflectsIsRegionalHA confirms that the
func TestPrepareVLMConfig_DeploymentTypeReflectsIsRegionalHA(t *testing.T) {
	setTestOCIImageEnv(t)

	cases := []struct {
		name               string
		isRegionalHA       bool
		wantDeploymentTyp  string
		wantEnableAAConfig bool
	}{
		{
			name:               "RegionalHA_NonShared_AADisabled",
			isRegionalHA:       true,
			wantDeploymentTyp:  vlm.DeploymentTypeNonSharedHA,
			wantEnableAAConfig: false,
		},
		{
			name:               "ZonalHA_Shared_AAEnabled",
			isRegionalHA:       false,
			wantDeploymentTyp:  vlm.DeploymentTypeSharedHA,
			wantEnableAAConfig: true,
		},
	}

	iops := int64(5000)
	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			params := &common.CreatePoolParams{
				AccountName:     "acct",
				SizeInBytes:     100 * 1024 * 1024 * 1024,
				PrimaryZone:     "ad1",
				SecondaryZone:   "ad2",
				MediatorZone:    "ad3",
				VendorSubNetID:  "subnet",
				CompartmentOCID: "comp",
				HAPairs:         1,
				IsRegionalHA:    tc.isRegionalHA,
				CustomPerformanceParams: &common.CustomPerformanceParams{
					ThroughputMibps: 128,
					Iops:            &iops,
				},
			}
			pool := &datamodel.Pool{
				BaseModel:      datamodel.BaseModel{UUID: "u1"},
				DeploymentName: "dep1",
				Name:           "pool1",
				Account:        &datamodel.Account{Name: "acct"},
			}

			cfg, err := prepareVLMConfig(params, pool, nil)
			require.NoError(tt, err)
			require.NotNil(tt, cfg)
			assert.Equal(tt, tc.wantDeploymentTyp, cfg.Deployment.DeploymentType,
				"prepareVLMConfig must propagate IsRegionalHA into Deployment.DeploymentType")
			assert.Equal(tt, tc.wantEnableAAConfig, cfg.Deployment.DeploymentConfigFlags.EnableAAConfig,
				"prepareVLMConfig must propagate the IsRegionalHA-derived AA flag into Deployment.DeploymentConfigFlags.EnableAAConfig (active-active is on iff shared HA)")
		})
	}
}

func TestOCIDeploymentConfig_HAModeAndAAConfigAreInverselyCoupled(t *testing.T) {
	setTestOCIImageEnv(t)

	build := func(isRegionalHA bool) vlm.DeploymentConfig {
		params := &common.CreatePoolParams{
			AccountName:     "acct",
			PrimaryZone:     "ad1",
			SecondaryZone:   "ad2",
			MediatorZone:    "ad3",
			CompartmentOCID: "comp",
			VendorSubNetID:  "subnet",
			DataNICSubnetID: "data-subnet",
			HAPairs:         1,
			IsRegionalHA:    isRegionalHA,
		}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-coupling"},
			Name:           "pool-coupling",
			DeploymentName: "dep-coupling",
		}
		return ociDeploymentConfig(params, pool, "100Gi", 256, 8192, vlm.OCIConfig{})
	}

	regional := build(true)
	zonal := build(false)

	// Direct inversion check: AA config is the boolean negation of "is regional HA".
	assert.NotEqual(t, regional.DeploymentConfigFlags.EnableAAConfig,
		zonal.DeploymentConfigFlags.EnableAAConfig,
		"EnableAAConfig must differ between the two HA modes; identical values means the IsRegionalHA branch was bypassed for this flag")

	// And the type must also differ — otherwise the test below devolves to
	// asserting two unrelated boolean swings happen to match, which would
	// not catch a copy-paste mistake.
	assert.NotEqual(t, regional.DeploymentType, zonal.DeploymentType,
		"DeploymentType must differ between the two HA modes")

	// Coupling: the regional pair must be (NonShared, AA-off) and the zonal
	// pair must be (Shared, AA-on). Asserting both pairs together catches the
	// case where someone flips just one half of the invariant.
	assert.Equal(t, vlm.DeploymentTypeNonSharedHA, regional.DeploymentType)
	assert.False(t, regional.DeploymentConfigFlags.EnableAAConfig,
		"regional HA implies non-shared deployment, which must NOT use active-active config")

	assert.Equal(t, vlm.DeploymentTypeSharedHA, zonal.DeploymentType)
	assert.True(t, zonal.DeploymentConfigFlags.EnableAAConfig,
		"zonal HA implies shared deployment, which MUST use active-active config")
}

// TestOCIDeploymentConfig_SerialNumberPrefixIsHardcoded pins the workflow's
// single source of truth for the VM serial-number prefix. The endpoint no
// longer forwards a caller value (the API field is accepted but ignored), and
// common.CreatePoolParams no longer carries a SerialNumberPrefix. So whatever
// the inputs look like, the workflow must always emit the same "955" + 15
// zeros value; any deviation here would silently change downstream VLM VM
// serial generation.
func TestOCIDeploymentConfig_SerialNumberPrefixIsHardcoded(t *testing.T) {
	setTestOCIImageEnv(t)

	params := &common.CreatePoolParams{
		AccountName:     "acct",
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		CompartmentOCID: "comp",
		VendorSubNetID:  "subnet",
		DataNICSubnetID: "data-subnet",
		HAPairs:         2,
		IsRegionalHA:    false,
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-prefix"},
		Name:           "pool-name-prefix",
		DeploymentName: "dep-name-prefix",
	}
	ociConfig := vlm.OCIConfig{
		CompartmentID:   params.CompartmentOCID,
		SubnetID:        params.VendorSubNetID,
		DataNICSubnetID: params.DataNICSubnetID,
	}

	got := ociDeploymentConfig(params, pool, "100Gi", 256, 8192, ociConfig)

	assert.Equal(t, "955000000000000000", got.SerialNumberPrefix,
		"workflow must emit the hardcoded 18-character VLM serial-number prefix; the value (\"955\" + 15 zeros) is what VLM expects and is not configurable from params or the API")
	assert.Equal(t, ociSerialNumberLeadingPrefix+ociSerialNumberPrefix, got.SerialNumberPrefix,
		"the emitted prefix must equal the concatenation of the two package-level constants; if either constant changes, this regression test will surface it loudly")
}

// TestOCIDeploymentConfig_LegacySerialPrefixIsExactly18Chars locks in the two
// constituent constants. Changing either is a coordinated rollout with VLM:
// the leading "955" is an OCI realm marker, and the trailing 15 zeros are
// load-bearing for VLM's serial-collision detection — neither should drift
// unilaterally.
func TestOCIDeploymentConfig_LegacySerialPrefixIsExactly18Chars(t *testing.T) {
	assert.Equal(t, "955", ociSerialNumberLeadingPrefix,
		"the leading prefix is the OCI realm marker; changing it requires a coordinated VLM rollout, not a unilateral constant edit")
	assert.Equal(t, "000000000000000", ociSerialNumberPrefix,
		"the trailing portion must remain exactly 15 zero digits — both the count and the all-zero pattern matter to VLM's serial-collision detection")
	assert.Equal(t, "955000000000000000", ociSerialNumberLeadingPrefix+ociSerialNumberPrefix,
		"the combined 18-character prefix must equal the historical value VLM has been seeing; any deviation breaks downstream VM serial generation silently")
}

func TestPrepareOCIDeleteVSAClusterDeploymentRequest(t *testing.T) {
	req := &vlm.DeleteVSAClusterDeploymentRequest{}
	pool := &datamodel.Pool{
		DeploymentName: "dep-1",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-from-pool"},
	}
	prepareOCIDeleteVSAClusterDeploymentRequest(req, pool, "tenancy-ocid")
	assert.Equal(t, vlm.OCICloud, req.CloudProvider)
	assert.Equal(t, "dep-1", req.DeploymentID)
	assert.Equal(t, "tenancy-ocid", req.ProjectID)
	require.NotNil(t, req.HyperScalerConfig)
	assert.Equal(t, "comp-from-pool", req.HyperScalerConfig.OCIConfig.CompartmentID)

	pool.ClusterDetails.CompartmentOCID = "comp-updated"
	prepareOCIDeleteVSAClusterDeploymentRequest(req, pool, "tenancy-2")
	assert.Equal(t, "comp-updated", req.HyperScalerConfig.OCIConfig.CompartmentID)
	assert.Equal(t, "tenancy-2", req.ProjectID)
}

func TestPrepareCreateVSAClusterDeploymentRequest_InitsNilLabels(t *testing.T) {
	setTestOCIImageEnv(t)
	req := &vlm.CreateVSAClusterDeploymentRequest{}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Labels: nil,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:              datamodel.BaseModel{UUID: "u1"},
		Name:                   "pn",
		PoolExternalIdentifier: "ocid.pool",
		Account:                &datamodel.Account{Name: "aname"},
	}
	cred := vlm.OntapCredentials{}
	prepareCreateVSAClusterDeploymentRequest(req, vlmConfig, cred, pool)
	require.NotNil(t, req.VLMConfig.Deployment.Labels)
	assert.Equal(t, "pn", req.VLMConfig.Deployment.Labels["pool_name"])
	assert.Equal(t, "ocid.pool", req.VLMConfig.Deployment.Labels["pool_ocid"])
	assert.Equal(t, "aname", req.VLMConfig.Deployment.Labels["account_id"])
}

func TestOCIDeletePoolWorkflow_Success(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid-del",
	}

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-del"},
		Name:           "p",
		DeploymentName: "dep-ocnv-abc",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-ocid"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	env.OnActivity("DeleteOnTapCredentialsForOCI", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCIDeletePoolWorkflow_VLMDeleteFailure(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid-del",
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-del"},
		Name:           "p",
		DeploymentName: "dep-ocnv-abc",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-ocid"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestOCIDeletePoolWorkflow_DBCleanupFailure(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid-del",
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-del"},
		Name:           "p",
		DeploymentName: "dep-ocnv-abc",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-ocid"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestOCICreatePoolWorkflow_Success(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024, // 1 TB
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// SaveVSANodeDetails (the unified, hyperscaler-agnostic activity now used by
	// the OCI workflow as well) takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	// UpdatePoolFields stamps build_info after the pool is marked READY.
	// Permissive mock — the dedicated TestOCICreatePoolWorkflow_PersistsBuildInfo
	// test below asserts on the actual captured arguments.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// TestOCICreatePoolWorkflow_VMRSEnabled_RunsVMRSBranch is the coverage and
// behavioral guard for the OCI_VMRS_ENABLED=true branch of
// OCICreatePoolWorkflow.Run. With the toggle off (the default exercised by
// TestOCICreatePoolWorkflow_Success) the whole block between
// workflow.SideEffect and prepareVLMConfig is skipped — meaning
// computeOCIVMRSInput, the dedicated short-timeout vmrsAO, and the
// IdentifyOCIResources activity dispatch are all reachable only when the
// toggle flips. This test flips it and asserts:
//
//  1. IdentifyOCIResources is invoked exactly once with per-VM capacity and
//     per-VM throughput derived from the pool's total size + topology
//     (regression guard for the per-VM unit contract documented on
//     computeOCIVMRSInput and IdentifyOCIResourcesRequest).
//  2. The Decision returned by the activity flows downstream so the rest of
//     the workflow runs to completion — i.e. enabling VMRS does not change
//     the workflow's terminal state.
func TestOCICreatePoolWorkflow_VMRSEnabled_RunsVMRSBranch(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVMRSEnabled(t, true)

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// CustomPerformanceParams is required to reach computeOCIVMRSInput's
	// arithmetic — validateOCIVMRSInput rejects nil CPP and any non-positive
	// ThroughputMibps before the math runs.
	//
	// IOPS must satisfy the pool validator's floor of 16 × ThroughputMibps
	// (see core/orchestrator/validators/pool_validator.go ValidateIops);
	// for 1024 MiBps that's 16384 IOPS. Any lower value would trip
	// prepareVLMConfig — invoked right after the VMRS branch — and fail
	// the workflow before this test can assert the IdentifyOCIResources
	// payload. computeOCIVMRSInput does not read Iops, so this value does
	// not affect the per-VM assertions below.
	iops := int64(16384)
	params := &common.CreatePoolParams{
		Name:        "test-pool-vmrs",
		AccountName: "test-account",
		SizeInBytes: 4 * 1_000_000_000_000, // 4 TB total
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     2,
		// IsRegionalHA=false (AA) → totalActiveVMs = 2 * HAPairs = 4.
		IsRegionalHA: false,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 1024,
			Iops:            &iops,
		},
	}

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid-vmrs"},
		Name:            "test-pool-vmrs",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	// IdentifyOCIResources is the activity that owns the OCI VMRS catalogue
	// selector. Asserting on its request payload pins the per-VM unit
	// contract end-to-end: 4 TB / 4 active VMs = 1.0 TB/VM, and 1024 MiB/s
	// converted to GB/s then divided by 4 = 0.268435456 GB/s/VM. Any
	// regression that re-introduces per-disk slicing or drops the AA
	// doubling will fail here.
	const expectedPerVMCapacityTB = 1.0
	const expectedPerVMThroughputGBs = 0.268435456
	vmrsDecision := &vmrs_oci.Decision{
		VMShape:   "VM.DenseIO.E4.Flex",
		OCPUs:     16,
		MemoryGBs: 256,
		VPU:       40,
		IOPS:      30000,
	}
	env.OnActivity("IdentifyOCIResources", mock.Anything,
		mock.MatchedBy(func(req activities.IdentifyOCIResourcesRequest) bool {
			return req.PoolUUID == pool.UUID &&
				approxEqual(req.PerVMCapacityTB, expectedPerVMCapacityTB, 1e-9) &&
				approxEqual(req.PerVMThroughputGBs, expectedPerVMThroughputGBs, 1e-9)
		}),
	).Return(vmrsDecision, nil).Once()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// approxEqual is a tiny float helper for mock.MatchedBy assertions on the
// VMRS per-VM inputs. Using a delta (instead of ==) avoids spurious test
// flakes from the MiB/s → GB/s float conversion inside computeOCIVMRSInput.
func approxEqual(a, b, delta float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= delta
}

// TestOCICreatePoolWorkflow_PersistsExternalSecretOnPoolCredentials is the regression
// guard for the steady-state OCI vault rehydration bug.
//
// CreateOnTapCredentialsForOCI is the only place that knows the Name / OCID /
// Version of the freshly created OCI Vault secret — that information exists
// nowhere else in the system. The workflow must copy it onto
// pool.PoolCredentials.ExternalSecret before exiting, so the JSONB credentials blob
// persisted by CreatedPool's se.UpdatedPool call carries the reference for
// every future operation that loads the pool from the DB.
//
// Without this write-back, downstream callers that build *models.Node via
// hyperscaler.CreateNodeForProvider — and the unified _saveNodeDetails, which
// also reads ExternalSecret off PoolCredentials — see
// PoolCredentials.ExternalSecret == nil, which gets copied (as nil) onto the
// runtime node, which causes _getProviderByNode → _getPasswordFromCacheOrOCIVault
// (when env.GetHyperscaler()=="oci") to return "OCI vault reference is empty"
// wrapped as ErrOCIResourceFetchError. That failure mode is what this test
// prevents from ever silently re-emerging.
func TestOCICreatePoolWorkflow_PersistsExternalSecretOnPoolCredentials(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	wfEnv := ts.NewTestWorkflowEnvironment()
	wfEnv.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(wfEnv)

	mockStorage := database.NewMockStorage(t)
	wfEnv.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	wfEnv.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// The secret coordinates the vault returns to the activity. These three
	// fields, taken together, are the entire payload that has to round-trip
	// from the activity through the workflow into the persisted pool row.
	expectedRef := &datamodel.ExternalCredRef{
		Name:               "ocnv-regression-secret",
		Version:            7,
		ExternalIdentifier: "ocid1.vaultsecret.oc1..regression",
	}

	// Stub CreateOnTapCredentialsForOCI to return the known ref. We do NOT
	// reuse registerOCICreatePoolOntapCredentialMocks here because that helper
	// uses .Maybe() and a different secret payload; this test wants a strict
	// expectation on the returned ref so the assertion below is unambiguous.
	wfEnv.OnActivity("CreateOnTapCredentialsForOCI", mock.Anything, mock.Anything).
		Return(&activities.OCICreatePoolCredentials{
			OntapCredentials: vlm.OntapCredentials{AdminPassword: "test-ontap-admin-password"},
			Secret:           expectedRef,
		}, nil)
	wfEnv.OnActivity("DeleteOnTapCredentialsForOCI", mock.Anything, mock.Anything).Return(nil).Maybe()

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
		// PoolCredentials must be non-nil (the workflow rejects nil at
		// line 191) but ExternalSecret is intentionally nil here — that's the
		// production starting state we are testing the workflow can heal.
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	wfEnv.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// Capture the pool that arrives at SaveVSANodeDetails as well as
	// CreatedPool. We assert against both: the former proves the mutation is
	// in place by the time the node-save activity runs (so the unified
	// _saveNodeDetails — which now reads pool.PoolCredentials.ExternalSecret
	// and propagates it onto the in-memory Node so the OCI vault lookup can
	// resolve — sees it), and the latter proves the mutation is in place by
	// the time the JSONB column is actually persisted via se.UpdatedPool.
	//
	// SaveVSANodeDetails takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	var saveNodeDetailsPool, createdPoolPool *datamodel.Pool
	wfEnv.OnActivity("SaveVSANodeDetails",
		mock.Anything,
		mock.MatchedBy(func(p *datamodel.Pool) bool {
			saveNodeDetailsPool = p
			return true
		}),
		mock.Anything, mock.Anything, mock.Anything,
	).Return((*datamodel.Node)(nil), nil)
	wfEnv.OnActivity("CreatedPool",
		mock.Anything,
		mock.MatchedBy(func(p *datamodel.Pool) bool {
			createdPoolPool = p
			return true
		}),
		mock.Anything,
	).Return(pool, nil)
	// The workflow now calls UpdatePoolFields twice on the happy path:
	//   1. "pool_credentials" — the OCI vault secret-ref write-back that
	//      this test owns.
	//   2. "build_info" — covered by TestOCICreatePoolWorkflow_PersistsBuildInfo.
	// We register a single permissive stub that inspects the updates map on
	// every call and captures the typed PoolCredentials payload from the
	// pool_credentials call. Temporal's data converter erases the in-workflow
	// *PoolCredentials into a generic map[string]interface{} on the wire (the
	// activity signature is map[string]interface{}), so we round-trip it
	// through JSON to recover the typed view that GORM would actually
	// persist into the JSONB column.
	var (
		capturedCredentialsUpdateUUID string
		capturedCredentialsUpdate     *datamodel.PoolCredentials
	)
	wfEnv.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			updates, ok := args[2].(map[string]interface{})
			if !ok {
				return
			}
			raw, ok := updates["pool_credentials"]
			if !ok {
				return
			}
			encoded, mErr := json.Marshal(raw)
			if mErr != nil {
				return
			}
			var pc datamodel.PoolCredentials
			if uErr := json.Unmarshal(encoded, &pc); uErr != nil {
				return
			}
			capturedCredentialsUpdate = &pc
			capturedCredentialsUpdateUUID, _ = args[1].(string)
		}).
		Return(nil)

	wfEnv.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, wfEnv.IsWorkflowCompleted())
	require.NoError(t, wfEnv.GetWorkflowError())

	// Primary assertion: the pool that reaches CreatedPool — and therefore
	// the pool whose JSONB credentials blob is persisted — must carry the
	// ExternalSecret produced by CreateOnTapCredentialsForOCI. Field-by-field
	// equality (not pointer identity) because Temporal serializes activity
	// arguments through its data converter.
	if assert.NotNil(t, createdPoolPool, "CreatedPool must be invoked with the workflow's pool") {
		if assert.NotNil(t, createdPoolPool.PoolCredentials, "PoolCredentials must be present on the persisted pool") {
			if assert.NotNil(t, createdPoolPool.PoolCredentials.ExternalSecret,
				"ExternalSecret must be persisted onto PoolCredentials before the workflow exits — "+
					"otherwise CreateNodeForProvider will re-hydrate nil and downstream OCI vault "+
					"lookups fail with \"OCI vault reference is empty\"") {
				assert.Equal(t, expectedRef.Name, createdPoolPool.PoolCredentials.ExternalSecret.Name)
				assert.Equal(t, expectedRef.Version, createdPoolPool.PoolCredentials.ExternalSecret.Version)
				assert.Equal(t, expectedRef.ExternalIdentifier, createdPoolPool.PoolCredentials.ExternalSecret.ExternalIdentifier)
			}
			// ExternalCertificate stays nil today (the cert flow is a TODO in
			// CreateOnTapCredentialsForOCI). When that lands, replace this
			// nil-check with a positive assertion mirroring ExternalSecret above.
			assert.Nil(t, createdPoolPool.PoolCredentials.ExternalCertificate,
				"ExternalCertificate is expected to be nil until the OCI certificate flow is wired up; "+
					"if this assertion fires, mirror the ExternalSecret assertions above for the cert ref")
		}
	}

	// Secondary assertion: the same mutation is visible to
	// SaveVSANodeDetails, which runs before CreatedPool. This pins the
	// ordering invariant — if anyone moves the credential write-back below
	// SaveVSANodeDetails, this assertion will catch it even though the
	// primary assertion above might still pass.
	if assert.NotNil(t, saveNodeDetailsPool, "SaveVSANodeDetails must be invoked with the workflow's pool") {
		if assert.NotNil(t, saveNodeDetailsPool.PoolCredentials) {
			assert.NotNil(t, saveNodeDetailsPool.PoolCredentials.ExternalSecret,
				"the credential write-back must happen before SaveVSANodeDetails runs, "+
					"so _saveNodeDetails (which reads ExternalSecret off the passed-in pool "+
					"and copies it onto the in-memory models.Node) sees the populated value")
		}
	}

	// Tertiary assertion — the actual DB-level persistence contract. The
	// primary/secondary assertions only prove the workflow's *in-memory*
	// pool object carries ExternalSecret as it flows through activity
	// invocations; they do NOT prove the JSONB column on disk gets the
	// reference. The only path that actually writes to pool_credentials
	// JSONB before CreatedPool refetches the row is an explicit
	// UpdatePoolFields(pool_credentials=...) activity call. Without this
	// assertion, the bug (CreatedPool refetching from DB and the subsequent
	// UpdatedPool re-serializing stale PoolCredentials, silently dropping
	// ExternalSecret) would slip past the in-memory checks above.
	if assert.NotNil(t, capturedCredentialsUpdate,
		"UpdatePoolFields must be invoked with a pool_credentials column update — "+
			"this is the only path that writes ExternalSecret into the JSONB column on disk; "+
			"without it CreatedPool's refetch overwrites the in-memory mutation and the "+
			"secret reference is silently lost between the workflow and the DB") {
		assert.Equal(t, "test-pool-uuid", capturedCredentialsUpdateUUID,
			"the UpdatePoolFields(pool_credentials=...) call must target the exact pool the workflow is creating")
		if assert.NotNil(t, capturedCredentialsUpdate.ExternalSecret,
			"pool_credentials.ExternalSecret must be present in the JSONB write so future "+
				"operations that load the pool from DB can resolve the OCI Vault secret") {
			assert.Equal(t, expectedRef.Name, capturedCredentialsUpdate.ExternalSecret.Name)
			assert.Equal(t, expectedRef.Version, capturedCredentialsUpdate.ExternalSecret.Version)
			assert.Equal(t, expectedRef.ExternalIdentifier, capturedCredentialsUpdate.ExternalSecret.ExternalIdentifier)
		}
		// ExternalCertificate stays nil today (cert flow is a TODO in
		// CreateOnTapCredentialsForOCI). When that lands, mirror the
		// ExternalSecret assertions above for the persisted cert ref.
		assert.Nil(t, capturedCredentialsUpdate.ExternalCertificate,
			"ExternalCertificate is expected to be nil in the JSONB write until the OCI "+
				"certificate flow is wired up; if this fires, mirror the ExternalSecret "+
				"assertions above for the persisted cert ref")
	}

	wfEnv.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_SetupError(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// Set up test data with invalid params to cause setup error
	params := &common.CreatePoolParams{
		Name:        "",
		AccountName: "",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	// Mock UpdateJob on storage (called by UpdateJobStatus activity)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete (setup may succeed but workflow should handle it)
	assert.True(t, env.IsWorkflowCompleted())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_EnsureJobStateError(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	// Mock GetJob activity to return ERROR state (should cause EnsureJobState to fail)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateERROR),
	}, nil).Maybe()
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete with error
	assert.True(t, env.IsWorkflowCompleted())
	// Should have error because job is in ERROR state
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_UpdateJobStatusError(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	// Mock GetJob activity - return NEW state for workflow job
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// SaveVSANodeDetails takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	// The OCI create-pool workflow now persists pool_credentials via
	// UpdatePoolFields BEFORE CreatedPool runs (write-back for the OCI
	// vault secret-ref). This test exercises the CreatedPool failure
	// branch; the upstream credentials write must succeed so the workflow
	// actually reaches CreatedPool. Permissive stub.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Pool)(nil), assert.AnError)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_SaveVSANodeDetailsFailure(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// SaveVSANodeDetails takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), assert.AnError)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_RunMethodCalled(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	// Mock GetJob activity - return NEW state for workflow job
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// SaveVSANodeDetails takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	// UpdatePoolFields stamps build_info after the pool is marked READY.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Assert workflow execution completed successfully
	// The Run method should be called and return nil, nil
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_ExpertModePasswordFromEnv(t *testing.T) {
	setTestOCIImageEnv(t)

	// Simulate OCI_EXPERT_MODE_PASSWORD being set before the binary starts by directly
	// overriding the package-level var (same package, so accessible).
	orig := ociExpertModePassword
	ociExpertModePassword = "preset-env-password"
	defer func() { ociExpertModePassword = orig }()

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	// CreateVSAExpertModeUser is always called; the preset password is used directly
	// without executing the GetExpertModeCredentialsForOCI activity.
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// GetExpertModeCredentialsForOCI must NOT be called when the env var is pre-set.
	// SaveVSANodeDetails takes (ctx, pool, vlmConfig, deploymentName, hostMap) → 5 matchers.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	// UpdatePoolFields stamps build_info after the pool is marked READY.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_CreateExpertModeCredentialsFails(t *testing.T) {
	setTestOCIImageEnv(t)
	// Ensure ociExpertModePassword is empty so the workflow takes the
	// GetExpertModeCredentialsForOCI activity path.
	setOCIExpertModePassword(t, "")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
		OciAdminPassword: &common.OciAdminPassword{
			Ocid:    "ocid1.vaultsecret.oc1..testadminpw",
			Version: 1,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// Simulate the activity returning an error (e.g. OCI secret fetch failure).
	env.OnActivity("GetExpertModeCredentialsForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return((*vlm.OntapCredentials)(nil), assert.AnError)
	// Rollback path: ErroredPool is called; SaveVSANodeDetails and CreatedPool are never reached.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_CreateVSAExpertModeUserFails(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	// Password comes from env var, but expert-mode user creation fails.
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, assert.AnError)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// Rollback path: ErroredPool is called; SaveVSANodeDetails and CreatedPool are never reached.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// TestOCICreatePoolWorkflow_RollbackDeletesOntapSecretAfterVLMCreateFails asserts that when
// CreateOnTapCredentialsForOCI has created a vault secret, a failure at CreateVSAClusterDeployment
// triggers rollback: VLM delete child workflow runs first (LIFO), then DeleteOnTapCredentialsForOCI.
func TestOCICreatePoolWorkflow_RollbackDeletesOntapSecretAfterVLMCreateFails(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		DeploymentName:  "dep-rollback-test",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "rollback-pass"},
	}

	env.OnActivity("CreateOnTapCredentialsForOCI", mock.Anything, mock.Anything).
		Return(&activities.OCICreatePoolCredentials{
			OntapCredentials: vlm.OntapCredentials{AdminPassword: "vault-generated-password"},
			Secret:           &datamodel.ExternalCredRef{Name: "dep-rollback-test-secret", Version: 1, ExternalIdentifier: "ocid1.vaultsecret.oc1..rollbacksecretocid"},
		}, nil).Once()
	env.OnActivity("DeleteOnTapCredentialsForOCI", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil).Once()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return((*vlm.CreateVSAClusterDeploymentResponse)(nil), assert.AnError)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_NilPoolCredentialsRejected(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Rollback fires when Run returns an error.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool credentials are required",
		"workflow should fail with the new pool-credentials guard, not some downstream error")
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_RunArgsValidation(t *testing.T) {
	validParams := &common.CreatePoolParams{Name: "p", AccountName: "a", HAPairs: 1}
	validPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "u"}, Name: "p"}

	cases := []struct {
		name             string
		args             []interface{}
		wantOriginalSubs string // substring expected in OriginalErr.Error()
		wantTrackingID   int    // 0 = don't assert; otherwise must match
	}{
		{
			name:             "ZeroArgs",
			args:             nil,
			wantOriginalSubs: "expected 2 args, got 0",
		},
		{
			name:             "OneArg",
			args:             []interface{}{validParams},
			wantOriginalSubs: "expected 2 args, got 1",
		},
		{
			name:             "Args0WrongType",
			args:             []interface{}{"not-params", validPool},
			wantOriginalSubs: "args[0] has unexpected type string",
		},
		{
			name:             "Args0TypedNil",
			args:             []interface{}{(*common.CreatePoolParams)(nil), validPool},
			wantOriginalSubs: "args[0] (*common.CreatePoolParams) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
		{
			name:             "Args1WrongType",
			args:             []interface{}{validParams, "not-pool"},
			wantOriginalSubs: "args[1] has unexpected type string",
		},
		{
			name:             "Args1TypedNil",
			args:             []interface{}{validParams, (*datamodel.Pool)(nil)},
			wantOriginalSubs: "args[1] (*datamodel.Pool) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			wf := &ociCreatePoolWorkflow{}
			out, customErr := wf.Run(nil, tc.args...)

			assert.Nil(tt, out, "validation failure should not return a payload")
			require.NotNil(tt, customErr, "expected a *vsaerrors.CustomError, got nil")
			require.NotNil(tt, customErr.OriginalErr, "OriginalErr should preserve the descriptive validation message")
			assert.Contains(tt, customErr.OriginalErr.Error(), tc.wantOriginalSubs)
			if tc.wantTrackingID != 0 {
				assert.Equal(tt, tc.wantTrackingID, customErr.TrackingID,
					"typed-nil cases should be classified as ErrResourceEmptyError so they aren't lumped with generic internal errors")
			}
		})
	}
}

func TestNewPoolBuildInfo(t *testing.T) {
	t.Run("StampsImagesAndCurrentOntapVersionForNonAllowlistedAccount", func(tt *testing.T) {
		withVSAImageOCIDs(tt, testVSAImageOCID, testMediatorImageOCID)
		withOCIOntapVersionDetails(tt, testOCIOntapVersion)
		// Empty allowlist => GetOntapVersionBasedOnAllowlisting returns Current.
		utils.SetExperimentalVersionAllowlistedAccountsForTesting("")
		tt.Cleanup(func() { utils.SetExperimentalVersionAllowlistedAccountsForTesting("") })

		stamp := time.Date(2026, time.May, 4, 10, 0, 0, 0, time.UTC)

		got := NewPoolBuildInfo(stamp, "non-allowlisted-account")

		require.NotNil(tt, got)
		assert.Equal(tt, testVSAImageOCID, got.VSABuildImage,
			"VSABuildImage must come from vsaImageName (VSA_IMAGE_NAME env, worker-side)")
		assert.Equal(tt, testMediatorImageOCID, got.MediatorBuildImage,
			"MediatorBuildImage must come from vsaMediatorImageName (VSA_MEDIATOR_IMAGE_NAME env, worker-side)")
		assert.Equal(tt, testOCIOntapVersion, got.OntapVersion,
			"non-allowlisted accounts must receive env.CurrentOntapVersionDetails after ExtractOntapVersion")
		assert.Equal(tt, stamp, got.BuildTimestamp,
			"BuildTimestamp must echo the caller-supplied time (replay-safety contract)")
		assert.Empty(tt, got.RbacFileHash, "RbacFileHash should remain empty until the OCI RBAC validation flow lands")
		assert.Empty(tt, got.RbacFileUrl, "RbacFileUrl should remain empty until the OCI RBAC validation flow lands")
	})

	t.Run("AllowlistedAccountReceivesExperimentalOntapVersion", func(tt *testing.T) {
		const (
			currentVersion      = "9.17.1P2"
			experimentalVersion = "9.18.1"
			allowlistedAccount  = "experimental-account"
		)
		withVSAImageOCIDs(tt, testVSAImageOCID, testMediatorImageOCID)

		origCurrent := envs.CurrentOntapVersionDetails
		origExperimental := envs.ExperimentalOntapVersionDetails
		envs.CurrentOntapVersionDetails = currentVersion
		envs.ExperimentalOntapVersionDetails = experimentalVersion
		tt.Cleanup(func() {
			envs.CurrentOntapVersionDetails = origCurrent
			envs.ExperimentalOntapVersionDetails = origExperimental
		})

		utils.SetExperimentalVersionAllowlistedAccountsForTesting(allowlistedAccount)
		tt.Cleanup(func() { utils.SetExperimentalVersionAllowlistedAccountsForTesting("") })

		got := NewPoolBuildInfo(time.Now(), allowlistedAccount)

		require.NotNil(tt, got)
		assert.Equal(tt, experimentalVersion, got.OntapVersion,
			"allowlisted accounts must receive env.ExperimentalOntapVersionDetails (matches the delete-path call to GetOntapVersionBasedOnAllowlisting)")
	})

	t.Run("EmptyVersionDetailsProduceEmptyOntapVersion", func(tt *testing.T) {
		withVSAImageOCIDs(tt, "", "")
		withOCIOntapVersionDetails(tt, "")
		stamp := time.Now()

		got := NewPoolBuildInfo(stamp, "any-account")

		require.NotNil(tt, got)
		assert.Empty(tt, got.VSABuildImage)
		assert.Empty(tt, got.MediatorBuildImage)
		assert.Empty(tt, got.OntapVersion,
			"with no current/experimental version configured, OntapVersion must be empty (no spurious default)")
		assert.Equal(tt, stamp, got.BuildTimestamp)
	})
}

func TestOCICreatePoolWorkflow_PersistsBuildInfo(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	// The OCI create-pool workflow now always invokes CreateOnTapCredentialsForOCI
	// before SaveVSANodeDetails / CreatedPool. Without a stub the activity tries
	// to initialise a real OCI Vault client and fails its retries, rolling the
	// workflow back before UpdatePoolFields is ever reached.
	registerOCICreatePoolOntapCredentialMocks(env)

	const wantPoolUUID = "test-pool-uuid-buildinfo"
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: wantPoolUUID},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	var (
		gotPoolUUID  string
		gotBuildInfo *datamodel.PoolBuildInfo
	)
	// The workflow now calls UpdatePoolFields twice on the happy path:
	// once for "pool_credentials" (the OCI vault secret-ref write-back)
	// and once for "build_info" (what this test owns). A single permissive
	// stub captures only the build_info call by filtering on the updates
	// map key; the pool_credentials call passes through with no capture
	// and a nil return. Cannot use .Once() here because two invocations
	// are expected, and cannot scope to "build_info" via MatchedBy at the
	// registration level without complicating the existing matcher shape.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			updates, ok := args[2].(map[string]interface{})
			if !ok {
				return
			}
			raw, ok := updates["build_info"]
			if !ok {
				return
			}
			gotPoolUUID, _ = args[1].(string)
			encoded, err := json.Marshal(raw)
			if err != nil {
				return
			}
			var bi datamodel.PoolBuildInfo
			if err := json.Unmarshal(encoded, &bi); err != nil {
				return
			}
			gotBuildInfo = &bi
		}).
		Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, wantPoolUUID, gotPoolUUID,
		"UpdatePoolFields must target the exact pool the workflow is creating")

	require.NotNil(t, gotBuildInfo,
		"UpdatePoolFields was never invoked with a build_info payload — the persistence step is missing")
	assert.Equal(t, testVSAImageOCID, gotBuildInfo.VSABuildImage,
		"VSABuildImage must reflect VSA_IMAGE_NAME from the worker container's env")
	assert.Equal(t, testMediatorImageOCID, gotBuildInfo.MediatorBuildImage,
		"MediatorBuildImage must reflect VSA_MEDIATOR_IMAGE_NAME from the worker container's env")
	assert.Equal(t, testOCIOntapVersion, gotBuildInfo.OntapVersion,
		"OntapVersion must reflect env.CurrentOntapVersionDetails for non-allowlisted accounts (mirrors the delete-path call to utils.GetOntapVersionBasedOnAllowlisting)")
	assert.False(t, gotBuildInfo.BuildTimestamp.IsZero(),
		"BuildTimestamp must be stamped with workflow.Now(ctx), not left as the zero value")

	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_BuildInfoPersistFailureIsNonFatal(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	// See sibling TestOCICreatePoolWorkflow_PersistsBuildInfo: without this stub
	// the OCI ontap-credential activity hits a real OCI Vault client init and
	// the workflow rolls back before reaching the UpdatePoolFields call this
	// test is targeting.
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid-buildinfo-fail"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	// The workflow makes TWO UpdatePoolFields calls on the happy path:
	//   1. "pool_credentials" — BEFORE CreatedPool. This is FATAL on failure
	//      (the pool isn't marked ready yet, so a rollback is the safe
	//      response). This test is not exercising that branch; it must
	//      succeed so the workflow reaches the build_info call below.
	//   2. "build_info" — AFTER CreatedPool. This is NON-FATAL on failure
	//      (the pool is already marked ready; failing the workflow would
	//      invalidate a usable pool). That is the contract under test.
	// Route the two calls by inspecting the updates-map key.
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything,
		mock.MatchedBy(func(updates map[string]interface{}) bool {
			_, ok := updates["pool_credentials"]
			return ok
		}),
	).Return(nil)

	updatePoolFieldsAttempted := false
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything,
		mock.MatchedBy(func(updates map[string]interface{}) bool {
			_, ok := updates["build_info"]
			return ok
		}),
	).Run(func(args mock.Arguments) { updatePoolFieldsAttempted = true }).
		Return(assert.AnError)

	erroredPoolCalled := false
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { erroredPoolCalled = true }).
		Return(pool, nil).
		Maybe()

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError(),
		"build_info persistence failure must be swallowed (logged as non-critical), not surfaced as a workflow error")

	assert.True(t, updatePoolFieldsAttempted,
		"the build_info UpdatePoolFields call must actually be exercised — otherwise this test proves nothing about the failure branch")
	assert.False(t, erroredPoolCalled,
		"ErroredPool/rollback must NOT fire on build_info persistence failure: the pool is already marked ready by CreatedPool, so a rollback here would invalidate a usable pool")
}

func TestOCIDeletePoolWorkflow_RunArgsValidation(t *testing.T) {
	validParams := &common.DeletePoolParams{}
	validPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "u"}, Name: "p"}

	cases := []struct {
		name             string
		args             []interface{}
		wantOriginalSubs string
		wantTrackingID   int
	}{
		{
			name:             "ZeroArgs",
			args:             nil,
			wantOriginalSubs: "expected 2 args, got 0",
		},
		{
			name:             "OneArg",
			args:             []interface{}{validParams},
			wantOriginalSubs: "expected 2 args, got 1",
		},
		{
			name:             "Args0WrongType",
			args:             []interface{}{"not-params", validPool},
			wantOriginalSubs: "args[0] has unexpected type string",
		},
		{
			name:             "Args0TypedNil",
			args:             []interface{}{(*common.DeletePoolParams)(nil), validPool},
			wantOriginalSubs: "args[0] (*common.DeletePoolParams) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
		{
			name:             "Args1WrongType",
			args:             []interface{}{validParams, "not-pool"},
			wantOriginalSubs: "args[1] has unexpected type string",
		},
		{
			name:             "Args1TypedNil",
			args:             []interface{}{validParams, (*datamodel.Pool)(nil)},
			wantOriginalSubs: "args[1] (*datamodel.Pool) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			wf := &ociDeletePoolWorkflow{}
			out, customErr := wf.Run(nil, tc.args...)

			assert.Nil(tt, out, "validation failure should not return a payload")
			require.NotNil(tt, customErr, "expected a *vsaerrors.CustomError, got nil")
			require.NotNil(tt, customErr.OriginalErr, "OriginalErr should preserve the descriptive validation message")
			assert.Contains(tt, customErr.OriginalErr.Error(), tc.wantOriginalSubs)
			if tc.wantTrackingID != 0 {
				assert.Equal(tt, tc.wantTrackingID, customErr.TrackingID,
					"typed-nil cases should be classified as ErrResourceEmptyError so they aren't lumped with generic internal errors")
			}
		})
	}
}

func withOCIVSASerialAllocationFlag(t *testing.T, enabled bool) {
	t.Helper()
	orig := ociVSASerialAllocationEnabled
	ociVSASerialAllocationEnabled = enabled
	t.Cleanup(func() { ociVSASerialAllocationEnabled = orig })
}

func withOCISerialAllocationEnv(t *testing.T, region, cell string) {
	t.Helper()
	origRegion := activities.RegionNumber
	origCell := ociCellNumber
	activities.RegionNumber = region
	ociCellNumber = cell
	t.Cleanup(func() {
		activities.RegionNumber = origRegion
		ociCellNumber = origCell
	})
}

func TestValidateNumericCode(t *testing.T) {
	cases := []struct {
		name    string
		code    string
		want    int
		wantErr bool
		errSubs string
	}{
		{name: "exact length numeric", code: "42", want: 2, wantErr: false},
		{name: "too short", code: "4", want: 2, wantErr: true, errSubs: "expected 2 digits, got 1"},
		{name: "too long", code: "423", want: 2, wantErr: true, errSubs: "expected 2 digits, got 3"},
		{name: "empty", code: "", want: 2, wantErr: true, errSubs: "expected 2 digits, got 0"},
		{name: "non-digit", code: "4a", want: 2, wantErr: true, errSubs: "must contain only digits"},
		{name: "leading zero ok", code: "01", want: 2, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			err := validateNumericCode("test", tc.code, tc.want)
			if !tc.wantErr {
				assert.NoError(tt, err)
				return
			}
			require.Error(tt, err)
			assert.True(tt, utilserrors.IsUserInputValidationErr(err),
				"length/charset failures must surface as 4xx UserInputValidationErr, not internal errors")
			assert.Contains(tt, err.Error(), tc.errSubs)
		})
	}
}

func TestBuildOCISerialPrefix(t *testing.T) {
	cases := []struct {
		name       string
		regionCode string
		cellCode   string
		want       string
		wantErr    bool
		errSubs    string
	}{
		{name: "valid", regionCode: "34", cellCode: "01", want: "9553401"},
		{name: "leading zero region", regionCode: "01", cellCode: "02", want: "9550102"},
		{name: "region too short", regionCode: "3", cellCode: "01", wantErr: true, errSubs: "region"},
		{name: "cell too long", regionCode: "34", cellCode: "001", wantErr: true, errSubs: "cell"},
		{name: "region non-digit", regionCode: "3a", cellCode: "01", wantErr: true, errSubs: "region"},
		{name: "cell empty", regionCode: "34", cellCode: "", wantErr: true, errSubs: "cell"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			got, err := buildOCISerialPrefix(tc.regionCode, tc.cellCode)
			if !tc.wantErr {
				require.NoError(tt, err)
				assert.Equal(tt, tc.want, got)
				assert.Len(tt, got, ociSerialPrefixLen,
					"prefix must be exactly PPP+RR+CC = 7 digits; downstream counter formatting depends on this width")
				return
			}
			require.Error(tt, err)
			assert.True(tt, utilserrors.IsUserInputValidationErr(err))
			assert.Contains(tt, err.Error(), tc.errSubs)
		})
	}
}

func TestAllocateOCIVMSerialNumbers_EarlyValidationErrors(t *testing.T) {
	cases := []struct {
		name      string
		region    string
		cell      string
		numHAPair int
		errSubs   string
	}{
		{name: "missing region", region: "", cell: "01", numHAPair: 1, errSubs: "region number is not set"},
		{name: "missing cell", region: "34", cell: "", numHAPair: 1, errSubs: "cell number is not set"},
		{name: "zero ha pairs", region: "34", cell: "01", numHAPair: 0, errSubs: "invalid VM count for serial allocation"},
		{name: "negative ha pairs", region: "34", cell: "01", numHAPair: -1, errSubs: "invalid VM count for serial allocation"},
		{name: "non-numeric region", region: "3a", cell: "01", numHAPair: 1, errSubs: "region"},
		{name: "wrong-length cell", region: "34", cell: "001", numHAPair: 1, errSubs: "cell"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			withOCISerialAllocationEnv(tt, tc.region, tc.cell)
			req := &vlm.CreateVSAClusterDeploymentRequest{
				VLMConfig: vlm.VLMConfig{
					Deployment: vlm.DeploymentConfig{NumHAPair: tc.numHAPair},
				},
			}

			err := allocateOCIVMSerialNumbers(nil, nil, req)

			require.Error(tt, err)
			assert.True(tt, utilserrors.IsUserInputValidationErr(err),
				"missing-config and bad-arity failures must surface as 4xx UserInputValidationErr so callers see a configuration problem, not an internal error")
			assert.Contains(tt, err.Error(), tc.errSubs)
			assert.Empty(tt, req.VLMConfig.Deployment.VMSerialNumbers,
				"failed allocation must not partially populate VMSerialNumbers")
		})
	}
}

func TestOCICreatePoolWorkflow_SerialAllocation_FlagOffSkipsAllocation(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVSASerialAllocationFlag(t, false)
	withOCISerialAllocationEnv(t, "34", "01")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	getNextSerialNumberCalled := false
	env.OnActivity("GetNextSerialNumber", mock.Anything).
		Run(func(args mock.Arguments) { getNextSerialNumberCalled = true }).
		Return(int64(0), nil).
		Maybe()

	var capturedDeployment vlm.DeploymentConfig
	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			req, ok := args.Get(1).(*vlm.CreateVSAClusterDeploymentRequest)
			if ok && req != nil {
				capturedDeployment = req.VLMConfig.Deployment
			}
		}).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	assert.False(t, getNextSerialNumberCalled,
		"GetNextSerialNumber must NOT be invoked when the allocation flag is off; "+
			"this is the load-bearing assertion for the rollout gate — if it fires, the workflow has bypassed the feature flag")

	assert.Equal(t, ociSerialNumberLeadingPrefix+ociSerialNumberPrefix, capturedDeployment.SerialNumberPrefix,
		"with the gate off, SerialNumberPrefix must remain the hardcoded \"955\"+15 zeros so VLM can generate VM serials from the prefix")
	assert.Empty(t, capturedDeployment.VMSerialNumbers,
		"VMSerialNumbers must be empty when allocation is gated off; VLM honors VMSerialNumbers only when SerialNumberPrefix is empty")
}

func TestOCICreatePoolWorkflow_SerialAllocation_FlagOnAllocatesSerials(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVSASerialAllocationFlag(t, true)
	withOCISerialAllocationEnv(t, "34", "01")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	const haPairs = 2
	wantNumVMs := haPairs * activities.VMsPerHAPair

	counters := []int64{1, 2, 3, 4}
	require.Equal(t, wantNumVMs, len(counters), "test must enumerate one counter per allocated VM")

	var counterIdx int
	env.OnActivity("GetNextSerialNumber", mock.Anything).
		Return(func(_ context.Context) (int64, error) {
			n := counters[counterIdx]
			counterIdx++
			return n, nil
		}).
		Times(wantNumVMs)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     haPairs,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	var capturedDeployment vlm.DeploymentConfig
	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			req, ok := args.Get(1).(*vlm.CreateVSAClusterDeploymentRequest)
			if ok && req != nil {
				capturedDeployment = req.VLMConfig.Deployment
			}
		}).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("UpdatePoolFields", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, wantNumVMs, counterIdx,
		"GetNextSerialNumber must be invoked exactly NumHAPair*VMsPerHAPair times when the flag is on; "+
			"a different count means the per-VM loop changed without updating this test")

	assert.Empty(t, capturedDeployment.SerialNumberPrefix,
		"with the gate on, SerialNumberPrefix must be cleared so VLM honors VMSerialNumbers instead")
	wantSerials := []string{
		"9553401" + "0000000000001",
		"9553401" + "0000000000002",
		"9553401" + "0000000000003",
		"9553401" + "0000000000004",
	}
	assert.Equal(t, wantSerials, capturedDeployment.VMSerialNumbers,
		"VMSerialNumbers must be PPP(955)+RR(34)+CC(01)+13-digit zero-padded counter for each VM, in allocation order")
}

func TestOCICreatePoolWorkflow_SerialAllocation_FlagOnMissingRegionRollsBack(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVSASerialAllocationFlag(t, true)
	withOCISerialAllocationEnv(t, "", "01")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		DeploymentName:  "dep-serial-rollback",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	erroredPoolCalled := false
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { erroredPoolCalled = true }).
		Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"allocation failure must surface as a workflow error so the caller doesn't observe a half-created pool")
	assert.True(t, erroredPoolCalled,
		"rollback must run on allocation failure to mark the pool as errored and undo the ONTAP credential side effect")
	mockVlmWorkflowClient.AssertNotCalled(t, "CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything)
}

func TestOCICreatePoolWorkflow_SerialAllocation_ActivityErrorRollsBack(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVSASerialAllocationFlag(t, true)
	withOCISerialAllocationEnv(t, "34", "01")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	env.OnActivity("GetNextSerialNumber", mock.Anything).
		Return(int64(0), errors.New("simulated GetNextSerialNumber DB failure"))

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		DeploymentName:  "dep-serial-activity-err",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	erroredPoolCalled := false
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { erroredPoolCalled = true }).
		Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"GetNextSerialNumber failure must surface as a workflow error so the pool is marked errored")
	assert.True(t, erroredPoolCalled,
		"rollback must run when the per-VM serial allocator fails — otherwise we'd ask VLM to deploy with no/partial serials")
	mockVlmWorkflowClient.AssertNotCalled(t, "CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything)
}

func TestOCICreatePoolWorkflow_SerialAllocation_CounterOverflowRollsBack(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")
	withOCIVSASerialAllocationFlag(t, true)
	withOCISerialAllocationEnv(t, "34", "01")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	registerOCICreatePoolOntapCredentialMocks(env)

	env.OnActivity("GetNextSerialNumber", mock.Anything).
		Return(ociSerialCounterMax, nil)

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		DeploymentName:  "dep-serial-overflow",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	erroredPoolCalled := false
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { erroredPoolCalled = true }).
		Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError(),
		"counter overflow must surface as a workflow error; silently emitting a 21-digit serial would break the VLM contract")
	assert.True(t, erroredPoolCalled,
		"rollback must run on counter overflow so the pool isn't left half-created")
	mockVlmWorkflowClient.AssertNotCalled(t, "CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything)
}
