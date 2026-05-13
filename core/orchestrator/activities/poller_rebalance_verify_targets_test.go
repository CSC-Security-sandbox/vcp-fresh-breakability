package activities

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrometheusTargetStringPort(t *testing.T) {
	p, ok := prometheusTargetStringPort("localhost:13001")
	require.True(t, ok)
	require.Equal(t, "13001", p)

	_, ok = prometheusTargetStringPort("")
	require.False(t, ok)

	p, ok = prometheusTargetStringPort("[::1]:13002")
	require.True(t, ok)
	require.Equal(t, "13002", p)

	p, ok = prometheusTargetStringPort("host-without-brackets:13003")
	require.True(t, ok)
	require.Equal(t, "13003", p)
}

func TestVerifyStagedPortsInPrometheusTargetsJSON(t *testing.T) {
	body := []byte(`[{"targets":["localhost:13001"],"labels":{"name":"cluster7-node-0002"}},{"targets":["localhost:13002"],"labels":{"name":"x"}}]`)
	err := verifyStagedPortsInPrometheusTargetsJSON(body, "harvest-l1", map[string]struct{}{
		"13001": {},
		"13002": {},
	})
	require.NoError(t, err)

	err = verifyStagedPortsInPrometheusTargetsJSON(body, "harvest-l1", map[string]struct{}{
		"13001": {},
		"13099": {},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "13099")

	err = verifyStagedPortsInPrometheusTargetsJSON([]byte(`not-json`), "lease-x", map[string]struct{}{"1": {}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal")
}
