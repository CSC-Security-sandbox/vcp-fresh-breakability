package detectors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/ccfe"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Integration tests in this file use in-memory VCP (database.NewTestStorage / SQLite) and an in-memory
// CCFE fixture over HTTP (list backup vaults path + JSON) via the real ccfe.Client.

// ccfeMemoryDB holds backup vault resource IDs per (GCP project number/id, location), mimicking CCFE list state.
type ccfeMemoryDB struct {
	mu   sync.Mutex
	data map[string][]string // key: project + "\x00" + location -> CCFE resource ids (name suffix)
}

func newCCFEMemoryDB() *ccfeMemoryDB {
	return &ccfeMemoryDB{data: make(map[string][]string)}
}

func ccfeKey(projectID, location string) string {
	return projectID + "\x00" + location
}

func (m *ccfeMemoryDB) set(projectID, location string, resourceIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := ccfeKey(projectID, location)
	cp := make([]string, len(resourceIDs))
	copy(cp, resourceIDs)
	m.data[k] = cp
}

// newCCFETestServer serves ListBackupVaults JSON from ccfeMemoryDB. Paths match CCFE:
// GET /v1beta1/projects/{project}/locations/{location}/backupVaults
func newCCFETestServer(t *testing.T, mem *ccfeMemoryDB) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := r.URL.Path
		const prefix = "/v1beta1/projects/"
		if !strings.HasPrefix(path, prefix) {
			http.NotFound(w, r)
			return
		}
		rest := strings.TrimPrefix(path, prefix)
		parts := strings.Split(rest, "/")
		if len(parts) != 4 || parts[1] != "locations" || parts[3] != "backupVaults" {
			http.NotFound(w, r)
			return
		}
		projectID, location := parts[0], parts[2]
		mem.mu.Lock()
		ids := append([]string(nil), mem.data[ccfeKey(projectID, location)]...)
		mem.mu.Unlock()

		type item struct {
			Name string `json:"name"`
		}
		var body struct {
			BackupVaults []item `json:"backupVaults"`
		}
		for _, id := range ids {
			body.BackupVaults = append(body.BackupVaults, item{
				Name: fmt.Sprintf("projects/%s/locations/%s/backupVaults/%s", projectID, location, id),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(body)
	}))
}

func newCCFEClientFromServer(t *testing.T, srv *httptest.Server) *ccfe.Client {
	t.Helper()
	getToken := func(context.Context) (string, error) { return "test-token", nil }
	return ccfe.NewClient(getToken,
		ccfe.WithBaseURL(srv.URL),
		ccfe.WithHTTPClient(srv.Client()),
		ccfe.WithTokenGetter(getToken),
	)
}

// vcpBackupVaultSeed describes one row to insert into the in-memory VCP SQLite DB.
type vcpBackupVaultSeed struct {
	AccountUUID string
	AccountName string // GCP project number / account name
	BVUUID      string
	BVName      string // resource id (matches CCFE list id)
	SrcRegion   string // pointer for SourceRegionName; empty => nil
	BackupReg   string // pointer for BackupRegionName when SrcRegion empty
}

func seedVCPBackupVaults(t *testing.T, store database.Storage, rows []vcpBackupVaultSeed) {
	t.Helper()
	db := store.DB()
	for _, row := range rows {
		accUUID := row.AccountUUID
		if accUUID == "" {
			accUUID = utils.RandomUUID()
		}
		bvUUID := row.BVUUID
		if bvUUID == "" {
			bvUUID = utils.RandomUUID()
		}
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: accUUID},
			Name:      row.AccountName,
		}
		require.NoError(t, db.Create(account).Error)

		bv := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: bvUUID},
			Name:      row.BVName,
			AccountID: account.ID,
		}
		if row.SrcRegion != "" {
			s := row.SrcRegion
			bv.SourceRegionName = &s
		}
		if row.BackupReg != "" {
			b := row.BackupReg
			bv.BackupRegionName = &b
		}
		require.NoError(t, db.Create(bv).Error)
	}
}

func sortLeakRecords(rec []model.LeakRecord) {
	sort.Slice(rec, func(i, j int) bool {
		if rec[i].Reason != rec[j].Reason {
			return rec[i].Reason < rec[j].Reason
		}
		if rec[i].ResourceID != rec[j].ResourceID {
			return rec[i].ResourceID < rec[j].ResourceID
		}
		return rec[i].Region < rec[j].Region
	})
}

func TestBackupVaultDetector_Integration_InMemoryVCPAndCCFE_NoLeaks(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("123456789", "us-central1", []string{"bv-alpha", "bv-beta"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "123456789", BVUUID: "uuid-alpha", BVName: "bv-alpha", SrcRegion: "us-central1"},
		{AccountName: "123456789", BVUUID: "uuid-beta", BVName: "bv-beta", SrcRegion: "us-central1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Integration_InCCFENotInVCP(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("proj-aa", "us-west1", []string{"only-in-ccfe", "in-both"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "proj-aa", BVUUID: "u1", BVName: "in-both", SrcRegion: "us-west1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "only-in-ccfe", records[0].ResourceID)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "proj-aa", records[0].ProjectID)
	assert.Equal(t, "us-west1", records[0].Region)
}

func TestBackupVaultDetector_Integration_InVCPNotInCCFE(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("888", "europe-west1", []string{"ccfe-one"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "888", BVUUID: "v-a", BVName: "ccfe-one", SrcRegion: "europe-west1"},
		{AccountName: "888", BVUUID: "v-b", BVName: "vcp-orphan", SrcRegion: "europe-west1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "vcp-orphan", records[0].ResourceName)
	assert.Equal(t, "v-b", records[0].ResourceID)
}

func TestBackupVaultDetector_Integration_MultipleProjectLocationGroups(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("p1", "us-central1", []string{"a1", "ccfe-extra-c1"})
	mem.set("p1", "us-east1", []string{"b1"})
	mem.set("p2", "asia-east1", []string{"c1", "c2"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "p1", BVUUID: "ua1", BVName: "a1", SrcRegion: "us-central1"},
		{AccountName: "p1", BVUUID: "ub1", BVName: "b1", SrcRegion: "us-east1"},
		{AccountName: "p2", BVUUID: "uc1", BVName: "c1", SrcRegion: "asia-east1"},
		{AccountName: "p2", BVUUID: "uc-missing-ccfe", BVName: "only-vcp-asia", SrcRegion: "asia-east1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	sortLeakRecords(records)
	expected := []model.LeakRecord{
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "ccfe-extra-c1", ResourceName: "ccfe-extra-c1", ProjectID: "p1", Region: "us-central1", Reason: ReasonInCCFENotInVCP},
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "uc-missing-ccfe", ResourceName: "only-vcp-asia", ProjectID: "p2", Region: "asia-east1", Reason: ReasonInVCPNotInCCFE, Extra: map[string]string{"uuid": "uc-missing-ccfe"}},
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "c2", ResourceName: "c2", ProjectID: "p2", Region: "asia-east1", Reason: ReasonInCCFENotInVCP},
	}
	sortLeakRecords(expected)
	assert.Equal(t, expected, records)
}

func TestBackupVaultDetector_Integration_BackupRegionFallbackMatchesCCFE(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("tenant-1", "us-west2", []string{"dr-vault"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "tenant-1", BVUUID: "bv-dr", BVName: "dr-vault", BackupReg: "us-west2"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Integration_SoftDeletedVCPVault_ReportedAsCCFEOnly(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("del-proj", "us-central1", []string{"gone-from-vcp", "still-there"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "del-proj", BVUUID: "u-gone", BVName: "gone-from-vcp", SrcRegion: "us-central1"},
		{AccountName: "del-proj", BVUUID: "u-ok", BVName: "still-there", SrcRegion: "us-central1"},
	})

	require.NoError(t, store.DB().Where("uuid = ?", "u-gone").Delete(&datamodel.BackupVault{}).Error)

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "gone-from-vcp", records[0].ResourceID)
}

func TestBackupVaultDetector_Integration_EmptyCCFEList_AllVCPReportedMissingInCCFE(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("empty-ccfe", "northamerica-northeast1", nil)
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "empty-ccfe", BVUUID: "x1", BVName: "lonely", SrcRegion: "northamerica-northeast1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "lonely", records[0].ResourceName)
}

func TestBackupVaultDetector_Integration_TwoAccountsSameRegion_Isolated(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("acct-a", "us-central1", []string{"vault-a"})
	mem.set("acct-b", "us-central1", []string{"vault-b", "ccfe-only-b"})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "acct-a", BVUUID: "va", BVName: "vault-a", SrcRegion: "us-central1"},
		{AccountName: "acct-b", BVUUID: "vb", BVName: "vault-b", SrcRegion: "us-central1"},
	})

	ctx := context.Background()
	d := NewBackupVaultDetector(newCCFEClientFromServer(t, srv))
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "ccfe-only-b", records[0].ResourceID)
	assert.Equal(t, "acct-b", records[0].ProjectID)
}
