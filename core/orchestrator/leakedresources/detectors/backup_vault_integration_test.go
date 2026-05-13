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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Integration tests use in-memory VCP (database.NewTestStorage / SQLite)
// and an in-memory CCFE fixture over HTTP via the real ccfe.Client
// wrapped in inProcessBackupVaultFetcher.

// ccfeVaultEntry is the seed record stored in the in-memory CCFE server.
// Both UUID and Name are required so the server can emit the full
// "internalBackupVaults" response shape (name path + netappUuid).
type ccfeVaultEntry struct {
	UUID string
	Name string // short resource name (segment after backupVaults/)
}

type ccfeMemoryDB struct {
	mu   sync.Mutex
	data map[string][]ccfeVaultEntry
}

func newCCFEMemoryDB() *ccfeMemoryDB {
	return &ccfeMemoryDB{data: make(map[string][]ccfeVaultEntry)}
}

func ccfeKey(projectID, location string) string {
	return projectID + "\x00" + location
}

func (m *ccfeMemoryDB) set(projectID, location string, entries []ccfeVaultEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]ccfeVaultEntry, len(entries))
	copy(cp, entries)
	m.data[ccfeKey(projectID, location)] = cp
}

func newCCFETestServer(t *testing.T, mem *ccfeMemoryDB) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		path := r.URL.Path
		const prefix = "/v1internal/projects/"
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
		entries := append([]ccfeVaultEntry(nil), mem.data[ccfeKey(projectID, location)]...)
		mem.mu.Unlock()

		type item struct {
			Name       string `json:"name"`
			NetappUUID string `json:"netappUuid,omitempty"`
		}
		var body struct {
			BackupVaults []item `json:"internalBackupVaults"`
		}
		for _, e := range entries {
			body.BackupVaults = append(body.BackupVaults, item{
				Name:       fmt.Sprintf("projects/%s/locations/%s/backupVaults/%s", projectID, location, e.Name),
				NetappUUID: e.UUID,
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

type inProcessBackupVaultFetcher struct {
	ccfeClient *ccfe.Client
}

func (f *inProcessBackupVaultFetcher) FetchCCFEBackupVaults(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedBackupVault, error) {
	result := make(map[string][]resourcescope.CachedBackupVault, len(locations))
	for _, loc := range locations {
		vaults, err := f.ccfeClient.ListBackupVaults(ctx, projectID, loc)
		if err != nil {
			continue
		}
		result[loc] = vaults
	}
	return result, nil
}

func newTestFetcher(t *testing.T, srv *httptest.Server) CCFEBackupVaultFetcher {
	t.Helper()
	return &inProcessBackupVaultFetcher{ccfeClient: newCCFEClientFromServer(t, srv)}
}

// staticBVLister returns a ProjectLocationLister with a fixed set of pairs.
func staticBVLister(pairs ...resourcescope.ProjectLocation) ProjectLocationLister {
	return &mockBVLister{pairs: pairs}
}

type vcpBackupVaultSeed struct {
	AccountUUID string
	AccountName string
	BVUUID      string
	BVName      string
	SrcRegion   string
	BackupReg   string
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
	mem.set("123456789", "us-central1", []ccfeVaultEntry{
		{UUID: "uuid-alpha", Name: "bv-alpha"},
		{UUID: "uuid-beta", Name: "bv-beta"},
	})
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
	lister := staticBVLister(plPair("123456789", "us-central1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Integration_InCCFENotInVCP(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("proj-aa", "us-west1", []ccfeVaultEntry{
		{UUID: "uuid-only-ccfe", Name: "only-in-ccfe"},
		{UUID: "uuid-both", Name: "in-both"},
	})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "proj-aa", BVUUID: "uuid-both", BVName: "in-both", SrcRegion: "us-west1"},
	})

	ctx := context.Background()
	lister := staticBVLister(plPair("proj-aa", "us-west1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "uuid-only-ccfe", records[0].ResourceID)
	assert.Equal(t, "only-in-ccfe", records[0].ResourceName)
	assert.Equal(t, model.ResourceTypeBackupVault, records[0].ResourceType)
	assert.Equal(t, "proj-aa", records[0].ProjectID)
	assert.Equal(t, "us-west1", records[0].Region)
}

func TestBackupVaultDetector_Integration_InVCPNotInCCFE(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("888", "europe-west1", []ccfeVaultEntry{
		{UUID: "v-a", Name: "ccfe-one"},
	})
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
	lister := staticBVLister(plPair("888", "europe-west1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "vcp-orphan", records[0].ResourceName)
	assert.Equal(t, "v-b", records[0].ResourceID)
}

func TestBackupVaultDetector_Integration_MultipleProjectLocationGroups(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("p1", "us-central1", []ccfeVaultEntry{
		{UUID: "ua1", Name: "a1"},
		{UUID: "uuid-ccfe-extra-c1", Name: "ccfe-extra-c1"},
	})
	mem.set("p1", "us-east1", []ccfeVaultEntry{
		{UUID: "ub1", Name: "b1"},
	})
	mem.set("p2", "asia-east1", []ccfeVaultEntry{
		{UUID: "uc1", Name: "c1"},
		{UUID: "uc2", Name: "c2"},
	})
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
	lister := staticBVLister(
		plPair("p1", "us-central1"),
		plPair("p1", "us-east1"),
		plPair("p2", "asia-east1"),
	)
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	sortLeakRecords(records)
	expected := []model.LeakRecord{
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "uc-missing-ccfe", ResourceName: "only-vcp-asia", ProjectID: "p2", Region: "asia-east1", Reason: ReasonInVCPNotInCCFE, Extra: map[string]string{"uuid": "uc-missing-ccfe"}},
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "uc2", ResourceName: "c2", ProjectID: "p2", Region: "asia-east1", Reason: ReasonInCCFENotInVCP, Extra: map[string]string{"uuid": "uc2"}},
		{ResourceType: model.ResourceTypeBackupVault, ResourceID: "uuid-ccfe-extra-c1", ResourceName: "ccfe-extra-c1", ProjectID: "p1", Region: "us-central1", Reason: ReasonInCCFENotInVCP, Extra: map[string]string{"uuid": "uuid-ccfe-extra-c1"}},
	}
	sortLeakRecords(expected)
	assert.Equal(t, expected, records)
}

func TestBackupVaultDetector_Integration_BackupRegionFallbackMatchesCCFE(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("tenant-1", "us-west2", []ccfeVaultEntry{
		{UUID: "bv-dr", Name: "dr-vault"},
	})
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
	lister := staticBVLister(plPair("tenant-1", "us-west2"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	assert.Empty(t, records)
}

func TestBackupVaultDetector_Integration_SoftDeletedVCPVault_ReportedAsCCFEOnly(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("del-proj", "us-central1", []ccfeVaultEntry{
		{UUID: "u-gone", Name: "gone-from-vcp"},
		{UUID: "u-ok", Name: "still-there"},
	})
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
	lister := staticBVLister(plPair("del-proj", "us-central1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "u-gone", records[0].ResourceID)
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
	lister := staticBVLister(plPair("empty-ccfe", "northamerica-northeast1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInVCPNotInCCFE, records[0].Reason)
	assert.Equal(t, "lonely", records[0].ResourceName)
	assert.Equal(t, "x1", records[0].ResourceID)
}

func TestBackupVaultDetector_Integration_TwoAccountsSameRegion_Isolated(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("acct-a", "us-central1", []ccfeVaultEntry{
		{UUID: "va", Name: "vault-a"},
	})
	mem.set("acct-b", "us-central1", []ccfeVaultEntry{
		{UUID: "vb", Name: "vault-b"},
		{UUID: "uuid-ccfe-only-b", Name: "ccfe-only-b"},
	})
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
	lister := staticBVLister(
		plPair("acct-a", "us-central1"),
		plPair("acct-b", "us-central1"),
	)
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "uuid-ccfe-only-b", records[0].ResourceID)
	assert.Equal(t, "acct-b", records[0].ProjectID)
}

// TestBackupVaultDetector_Integration_ZeroVCPVaults_CCFEOnlyDetected is
// the key test: even with no VCP backup vault rows, CCFE-only vaults
// are still detected.
func TestBackupVaultDetector_Integration_ZeroVCPVaults_CCFEOnlyDetected(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("orphan-proj", "us-central1", []ccfeVaultEntry{
		{UUID: "leaked-uuid", Name: "leaked-vault"},
	})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	lister := staticBVLister(plPair("orphan-proj", "us-central1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, ReasonInCCFENotInVCP, records[0].Reason)
	assert.Equal(t, "leaked-uuid", records[0].ResourceID)
	assert.Equal(t, "leaked-vault", records[0].ResourceName)
	assert.Equal(t, "orphan-proj", records[0].ProjectID)
}

// TestBackupVaultDetector_Integration_SameNameDifferentUUID exercises
// the key correctness property of UUID-based diffing: a vault deleted
// and recreated under the same name appears as both an in_ccfe_not_in_vcp
// (new UUID) and an in_vcp_not_in_ccfe (old UUID) record.
func TestBackupVaultDetector_Integration_SameNameDifferentUUID(t *testing.T) {
	mem := newCCFEMemoryDB()
	mem.set("proj-x", "us-central1", []ccfeVaultEntry{
		{UUID: "new-uuid", Name: "vault-a"}, // recreated under same name
	})
	srv := newCCFETestServer(t, mem)
	defer srv.Close()

	logger := log.NewLogger()
	store, err := database.NewTestStorage(logger)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	seedVCPBackupVaults(t, store, []vcpBackupVaultSeed{
		{AccountName: "proj-x", BVUUID: "old-uuid", BVName: "vault-a", SrcRegion: "us-central1"},
	})

	ctx := context.Background()
	lister := staticBVLister(plPair("proj-x", "us-central1"))
	d := NewBackupVaultDetector(newTestFetcher(t, srv), lister)
	records, err := d.Detect(ctx, store)
	require.NoError(t, err)
	require.Len(t, records, 2)
	reasons := map[string]string{}
	for _, r := range records {
		reasons[r.ResourceID] = r.Reason
	}
	assert.Equal(t, ReasonInCCFENotInVCP, reasons["new-uuid"])
	assert.Equal(t, ReasonInVCPNotInCCFE, reasons["old-uuid"])
}
