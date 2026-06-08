package common

import (
	"testing"
)

func TestCommandSpec(t *testing.T) {
	cmd := CommandSpec{
		Cmd:  "go",
		Args: []string{"build", "./..."},
	}
	if cmd.Cmd != "go" {
		t.Errorf("expected cmd 'go', got %q", cmd.Cmd)
	}
	if len(cmd.Args) != 2 {
		t.Errorf("expected 2 args, got %d", len(cmd.Args))
	}
}

func TestEcosystemAdapterHasCapability(t *testing.T) {
	adapter := BuildGoAdapter()

	tests := []struct {
		cap      CapabilityType
		expected bool
	}{
		{CapabilityBuild, true},
		{CapabilityTest, true},
		{CapabilityInstall, true},
		{CapabilityVet, true},
		{CapabilityAPIDiff, false},
		{CapabilityReleaseNote, false},
	}

	for _, tc := range tests {
		result := adapter.HasCapability(tc.cap)
		if result != tc.expected {
			t.Errorf("HasCapability(%q) = %v, expected %v", tc.cap, result, tc.expected)
		}
	}
}

func TestGoAdapter(t *testing.T) {
	adapter := BuildGoAdapter()

	if adapter.Name != "go" {
		t.Errorf("expected name 'go', got %q", adapter.Name)
	}
	if adapter.DisplayName != "Go" {
		t.Errorf("expected display name 'Go', got %q", adapter.DisplayName)
	}
	if adapter.PackageManager != "go mod" {
		t.Errorf("expected package manager 'go mod', got %q", adapter.PackageManager)
	}

	if len(adapter.Capabilities) == 0 {
		t.Fatal("expected capabilities, got none")
	}

	buildCap := adapter.GetCapability(CapabilityBuild)
	if buildCap == nil {
		t.Fatal("build capability not found")
	}
	if !buildCap.Supported {
		t.Error("build capability should be supported")
	}
	if len(buildCap.Commands) == 0 {
		t.Error("build capability should have commands")
	}
}

func TestNpmAdapterPlaceholder(t *testing.T) {
	adapter := BuildNpmAdapter()

	if adapter.Name != "npm" {
		t.Errorf("expected name 'npm', got %q", adapter.Name)
	}

	// All capabilities should be unsupported
	for _, cap := range adapter.Capabilities {
		if cap.Supported {
			t.Errorf("capability %q should be unsupported", cap.Capability)
		}
		if cap.Reason == "" {
			t.Errorf("unsupported capability %q should have a reason", cap.Capability)
		}
	}
}

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	goAdapter := BuildGoAdapter()

	if err := reg.Register(goAdapter); err != nil {
		t.Fatalf("failed to register adapter: %v", err)
	}

	retrieved := reg.Get("go")
	if retrieved == nil {
		t.Fatal("expected to retrieve 'go' adapter, got nil")
	}
	if retrieved.Name != "go" {
		t.Errorf("expected name 'go', got %q", retrieved.Name)
	}
}

func TestRegistryRegisterDuplicateFails(t *testing.T) {
	reg := NewRegistry()
	adapter1 := BuildGoAdapter()
	adapter2 := BuildGoAdapter()

	if err := reg.Register(adapter1); err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	if err := reg.Register(adapter2); err == nil {
		t.Fatal("expected duplicate register to fail")
	}
}

func TestRegistryGetOrFail(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())

	adapter, err := reg.GetOrFail("go")
	if err != nil {
		t.Errorf("failed to get 'go': %v", err)
	}
	if adapter == nil {
		t.Fatal("adapter should not be nil")
	}

	_, err = reg.GetOrFail("unknown")
	if err == nil {
		t.Fatal("expected error for unknown ecosystem")
	}
}

func TestRegistryGetCommands(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())

	cmds := reg.GetCommands("go", CapabilityBuild)
	if len(cmds) == 0 {
		t.Fatal("expected build commands for Go")
	}
	if cmds[0].Cmd != "go" {
		t.Errorf("expected cmd 'go', got %q", cmds[0].Cmd)
	}
}

func TestRegistryGetCommandsUnsupported(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())

	// API_DIFF is not supported, should return empty
	cmds := reg.GetCommands("go", CapabilityAPIDiff)
	if len(cmds) != 0 {
		t.Errorf("expected no commands for unsupported capability, got %d", len(cmds))
	}
}

func TestRegistryGetCommandsUnknownEcosystem(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())

	// Unknown ecosystem should not crash, return empty
	cmds := reg.GetCommands("rust", CapabilityBuild)
	if len(cmds) != 0 {
		t.Errorf("expected no commands for unknown ecosystem, got %d", len(cmds))
	}
}

func TestDefaultRegistry(t *testing.T) {
	reg := GetDefaultRegistry()

	adapters := reg.ListAdapters()
	expectedEcosystems := map[string]bool{"go": true, "npm": true, "pip": true}

	for expected := range expectedEcosystems {
		if adapters[expected] == nil {
			t.Errorf("expected ecosystem %q in default registry", expected)
		}
	}
}

func TestDefaultRegistryGoResolvesCommands(t *testing.T) {
	reg := GetDefaultRegistry()

	go_adapter, err := reg.GetOrFail("go")
	if err != nil {
		t.Fatalf("failed to get Go adapter: %v", err)
	}

	if !go_adapter.HasCapability(CapabilityBuild) {
		t.Error("Go adapter should support build capability")
	}

	cmds := reg.GetCommands("go", CapabilityBuild)
	if len(cmds) == 0 {
		t.Fatal("expected build commands for Go")
	}
}

func TestDefaultRegistryNpmAbstains(t *testing.T) {
	reg := GetDefaultRegistry()

	// npm should ABSTAIN from all capabilities
	for _, cap := range []CapabilityType{
		CapabilityInstall, CapabilityBuild, CapabilityTest,
		CapabilityVet, CapabilityAPIDiff, CapabilityReleaseNote,
	} {
		cmds := reg.GetCommands("npm", cap)
		if len(cmds) != 0 {
			t.Errorf("npm should abstain from %q, got %d commands", cap, len(cmds))
		}
	}
}

func TestRegistryListAdapters(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())
	reg.Register(BuildNpmAdapter())

	adapters := reg.ListAdapters()
	if len(adapters) != 2 {
		t.Errorf("expected 2 adapters, got %d", len(adapters))
	}

	if adapters["go"] == nil || adapters["npm"] == nil {
		t.Fatal("expected both Go and npm adapters")
	}
}

func TestRegistryJSON(t *testing.T) {
	reg := NewRegistry()
	reg.Register(BuildGoAdapter())

	jsonData, err := reg.ToJSON()
	if err != nil {
		t.Fatalf("failed to marshal to JSON: %v", err)
	}

	if len(jsonData) == 0 {
		t.Fatal("expected non-empty JSON")
	}

	reg2 := NewRegistry()
	if err := reg2.FromJSON(jsonData); err != nil {
		t.Fatalf("failed to unmarshal from JSON: %v", err)
	}

	adapter := reg2.Get("go")
	if adapter == nil {
		t.Fatal("expected Go adapter after JSON round-trip")
	}
}

func TestFailClosedBehavior(t *testing.T) {
	reg := GetDefaultRegistry()

	// Unknown ecosystem should fail gracefully
	_, err := reg.GetOrFail("rust")
	if err == nil {
		t.Fatal("expected error for unknown ecosystem")
	}

	// But GetCommands should not crash
	cmds := reg.GetCommands("rust", CapabilityBuild)
	if len(cmds) != 0 {
		t.Errorf("expected no commands for unknown ecosystem, got %d", len(cmds))
	}

	// npm build should be unsupported
	npmAdapter := reg.Get("npm")
	if npmAdapter == nil {
		t.Fatal("expected npm adapter in default registry")
	}

	buildCap := npmAdapter.GetCapability(CapabilityBuild)
	if buildCap == nil {
		t.Fatal("expected build capability for npm")
	}
	if buildCap.Supported {
		t.Error("npm build should be unsupported")
	}
}
