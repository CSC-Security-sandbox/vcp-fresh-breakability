package workflows

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

// vpgResourceIDPattern matches API constraint: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$
var vpgResourceIDPattern = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)

func TestTransitionVPGNameFromPoolName(t *testing.T) {
	tests := []struct {
		name     string
		poolName string
		want     string
	}{
		{
			name:     "normal pool name",
			poolName: "mypool",
			want:     "mypool-vpg",
		},
		{
			name:     "pool name with hyphens",
			poolName: "my-pool-01",
			want:     "my-pool-01-vpg",
		},
		{
			name:     "pool name with spaces",
			poolName: "my pool",
			want:     "my-pool-vpg",
		},
		{
			name:     "pool name with underscores",
			poolName: "my_pool",
			want:     "my-pool-vpg",
		},
		{
			name:     "uppercase normalized to lowercase",
			poolName: "MyPool",
			want:     "mypool-vpg",
		},
		{
			name:     "empty string fallback",
			poolName: "",
			want:     "p-vpg",
		},
		{
			name:     "only invalid chars fallback",
			poolName: "!!!@@@###",
			want:     "p-vpg",
		},
		{
			name:     "empty after sanitization fallback",
			poolName: "---",
			want:     "p-vpg",
		},
		{
			name:     "starts with number gets p- prefix",
			poolName: "9pool",
			want:     "p-9pool-vpg",
		},
		{
			name:     "invalid leading char stripped leaves valid name",
			poolName: ".pool",
			want:     "pool-vpg",
		},
		{
			name:     "mixed content with invalid chars removed",
			poolName: "pool_01 (production)",
			want:     "pool-01-production-vpg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TransitionVPGNameFromPoolName(tt.poolName)
			assert.Equal(t, tt.want, got, "TransitionVPGNameFromPoolName(%q)", tt.poolName)
			assert.True(t, len(got) <= 63, "result must be at most 63 chars")
			assert.True(t, vpgResourceIDPattern.MatchString(got), "result must match VPG resourceId pattern")
		})
	}
}

func TestTransitionVPGNameFromPoolName_LongNameTruncated(t *testing.T) {
	// 60-char base + "-vpg" would be 64; helper limits base to 59 so result is 63
	longBase := ""
	for i := 0; i < 60; i++ {
		longBase += "a"
	}
	got := TransitionVPGNameFromPoolName(longBase)
	assert.LessOrEqual(t, len(got), 63, "result must be at most 63 chars")
	assert.True(t, vpgResourceIDPattern.MatchString(got), "result must match VPG resourceId pattern")
	assert.Equal(t, "-vpg", got[len(got)-4:], "result must end with -vpg")
}

func TestTransitionVPGNameFromPoolName_AllEdgeCases(t *testing.T) {
	// Fallback: empty or invalid -> p-vpg
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName(""))
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName("   "))
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName("___"))

	// Normal
	assert.Equal(t, "poolname-vpg", TransitionVPGNameFromPoolName("poolname"))
	assert.Equal(t, "test-pool-vpg", TransitionVPGNameFromPoolName("test-pool"))

	// Trailing hyphens trimmed so last char is [a-z0-9]
	assert.Equal(t, "ab-vpg", TransitionVPGNameFromPoolName("ab---"))

	// Single non-[a-z] char becomes p-vpg (add "p-", then trim leaves empty)
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName("9"))
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName("."))

	// Long name that after first truncation+trim becomes empty (line 45)
	longHyphens := ""
	for i := 0; i < 60; i++ {
		longHyphens += "-"
	}
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName(longHyphens))

	// First char not [a-z]: add "p-", then truncate and trim can leave non-empty (line 51-52, 58)
	assert.Equal(t, "p-9pool-vpg", TransitionVPGNameFromPoolName("9pool"))
	// After TrimRight(s, "-") we get empty (line 61-62)
	assert.Equal(t, "p-vpg", TransitionVPGNameFromPoolName("9---"))
	// Long base that after "p-" prefix exceeds maxBaseLen, truncate and TrimRight leaves empty (line 65, 70)
	longNines := "9"
	for i := 0; i < 60; i++ {
		longNines += "a"
	}
	got := TransitionVPGNameFromPoolName(longNines)
	assert.True(t, len(got) <= 63)
	assert.True(t, vpgResourceIDPattern.MatchString(got))
	assert.Equal(t, "-vpg", got[len(got)-4:])
}
