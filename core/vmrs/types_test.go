package vmrs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindVMsByType_LSSDSuffixFallback(t *testing.T) {
	vms := []VMPerfLimit{
		{VMType: "c3-standard-4-lssd", RelativeCost: 1.0},
		{VMType: "c3-standard-8-lssd", RelativeCost: 2.0},
	}

	t.Run("stripped names resolve to lssd catalog entries", func(t *testing.T) {
		cur, next := FindVMsByType(vms, "c3-standard-4", "c3-standard-8-lssd")
		assert.NotNil(t, cur)
		assert.Equal(t, "c3-standard-4-lssd", cur.VMType)
		assert.NotNil(t, next)
		assert.Equal(t, "c3-standard-8-lssd", next.VMType)
	})

	t.Run("canonical names still match exactly", func(t *testing.T) {
		cur, next := FindVMsByType(vms, "c3-standard-4-lssd", "c3-standard-8-lssd")
		assert.NotNil(t, cur)
		assert.NotNil(t, next)
	})

	t.Run("same stripped type compares to same VMRS row", func(t *testing.T) {
		cur, next := FindVMsByType(vms, "c3-standard-4", "c3-standard-4")
		assert.NotNil(t, cur)
		assert.NotNil(t, next)
		assert.Same(t, cur, next)
	})

	t.Run("unknown base does not match", func(t *testing.T) {
		cur, next := FindVMsByType(vms, "unknown-type", "c3-standard-4-lssd")
		assert.Nil(t, cur)
		assert.NotNil(t, next)
	})
}

// Regression: if both a bare and -lssd row ever coexist, exact name must win (no wrong tier).
func TestResolveVMPerfLimit_PrefersExactRowWhenBareAndLssdBothExist(t *testing.T) {
	vms := []VMPerfLimit{
		{VMType: "c3-standard-4", RelativeCost: 0.5},
		{VMType: "c3-standard-4-lssd", RelativeCost: 1.0},
	}
	got := resolveVMPerfLimit(vms, "c3-standard-4")
	assert.NotNil(t, got)
	assert.Equal(t, "c3-standard-4", got.VMType, "must not upgrade bare name to -lssd row when both exist")
	gotL := resolveVMPerfLimit(vms, "c3-standard-4-lssd")
	assert.NotNil(t, gotL)
	assert.Equal(t, "c3-standard-4-lssd", gotL.VMType)
}

func TestCanonicalVMTypeInCatalog(t *testing.T) {
	vms := []VMPerfLimit{{VMType: "c3-standard-4-lssd", RelativeCost: 1.0}}

	s, ok := CanonicalVMTypeInCatalog(vms, "")
	assert.False(t, ok)
	assert.Equal(t, "", s)

	_, ok = CanonicalVMTypeInCatalog(vms, "missing")
	assert.False(t, ok)

	s, ok = CanonicalVMTypeInCatalog(vms, "c3-standard-4")
	assert.True(t, ok)
	assert.Equal(t, "c3-standard-4-lssd", s)

	s, ok = CanonicalVMTypeInCatalog(vms, "c3-standard-4-lssd")
	assert.True(t, ok)
	assert.Equal(t, "c3-standard-4-lssd", s)

	// Catalog vm_type matches pool string when canonical forms agree (exact or stripped).
	catalogRow := "c3-standard-4-lssd"
	got, ok := CanonicalVMTypeInCatalog(vms, "c3-standard-4-lssd")
	assert.True(t, ok)
	assert.Equal(t, catalogRow, got)
	got, ok = CanonicalVMTypeInCatalog(vms, "c3-standard-4")
	assert.True(t, ok)
	assert.Equal(t, catalogRow, got)
	assert.NotEqual(t, "c3-standard-8-lssd", got)
}

func TestResolveVMPerfLimit_Edges(t *testing.T) {
	vms := []VMPerfLimit{{VMType: "c3-standard-4-lssd", RelativeCost: 1.0}}
	assert.Nil(t, resolveVMPerfLimit(vms, ""))
	assert.Nil(t, resolveVMPerfLimit(vms, "missing"))
	got := resolveVMPerfLimit(vms, "c3-standard-4")
	assert.NotNil(t, got)
	assert.Equal(t, "c3-standard-4-lssd", got.VMType)
	assert.Nil(t, resolveVMPerfLimit(vms, "c3-standard-4-lssd-extra"), "no double suffix")
}
