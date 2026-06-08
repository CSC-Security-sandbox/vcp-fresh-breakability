// File: ecosystems.go
// Provides lightweight adapter interfaces for multi-ecosystem breakability analysis.
//
// Adapters declare their capabilities (install, build, test, api-diff, release-note) and
// the framework uses these to orchestrate ecosystem-specific commands. This enables the Go MVP
// to work with any ecosystem by delegating to the appropriate adapter.
//
// The design is intentionally simple: each ecosystem declares what it can do, and the framework
// fails safely (ABSTAIN) on unknown ecosystems or unsupported capabilities.
package common

import (
	"encoding/json"
	"fmt"
)

// CapabilityType represents a capability that an ecosystem can declare support for.
type CapabilityType string

const (
	CapabilityInstall   CapabilityType = "install"    // Package manager install
	CapabilityBuild     CapabilityType = "build"      // Language build
	CapabilityTest      CapabilityType = "test"       // Language test
	CapabilityAPIDiff   CapabilityType = "api_diff"   // API signature comparison
	CapabilityReleaseNote CapabilityType = "release_note" // Release note detection
	CapabilityVet       CapabilityType = "vet"        // Linting/static analysis
)

// CommandSpec defines a command specification for running in an ecosystem.
type CommandSpec struct {
	Cmd         string            `json:"cmd"`                   // Command to run (e.g., "go build ./...")
	Args        []string          `json:"args"`                  // Additional arguments
	Env         map[string]string `json:"env"`                   // Environment overrides
	TimeoutSec  int               `json:"timeout_sec"`           // Timeout in seconds
	Description string            `json:"description"`           // Human-readable description
}

// EcosystemCapability declares a single capability for an ecosystem.
type EcosystemCapability struct {
	Capability CapabilityType `json:"capability"`  // Capability type
	Supported  bool           `json:"supported"`   // If false, this capability is unsupported/ABSTAIN
	Commands   []CommandSpec  `json:"commands"`    // Commands to run for this capability
	Reason     string         `json:"reason"`      // Why capability is unsupported (if not supported)
}

// EcosystemAdapter represents an adapter for a single ecosystem (Go, npm, pip, etc.).
type EcosystemAdapter struct {
	Name           string                  `json:"name"`            // "go", "npm", "pip", "rust", etc.
	DisplayName    string                  `json:"display_name"`    // "Go", "npm", "Python"
	PackageManager string                  `json:"package_manager"` // "go mod", "npm", "pip", "cargo"
	Capabilities   []EcosystemCapability   `json:"capabilities"`    // Declared capabilities
	FilePatterns   []string                `json:"file_patterns"`   // Glob patterns to detect ecosystem
	Metadata       map[string]interface{}  `json:"metadata"`        // Extra metadata
}

// HasCapability checks if this adapter supports a capability.
func (a *EcosystemAdapter) HasCapability(cap CapabilityType) bool {
	for _, c := range a.Capabilities {
		if c.Capability == cap {
			return c.Supported
		}
	}
	return false
}

// GetCapability returns capability details, or nil if not declared.
func (a *EcosystemAdapter) GetCapability(cap CapabilityType) *EcosystemCapability {
	for i, c := range a.Capabilities {
		if c.Capability == cap {
			return &a.Capabilities[i]
		}
	}
	return nil
}

// Registry provides lookup and capability resolution for ecosystem adapters.
type Registry struct {
	adapters map[string]*EcosystemAdapter
}

// NewRegistry creates a fresh (empty) registry.
func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[string]*EcosystemAdapter),
	}
}

// Register adds an adapter to the registry.
func (r *Registry) Register(adapter *EcosystemAdapter) error {
	if _, exists := r.adapters[adapter.Name]; exists {
		return fmt.Errorf("ecosystem %q already registered", adapter.Name)
	}
	r.adapters[adapter.Name] = adapter
	return nil
}

// Get returns an adapter by ecosystem name, or nil if not registered.
func (r *Registry) Get(ecosystem string) *EcosystemAdapter {
	return r.adapters[ecosystem]
}

// GetOrFail returns an adapter by ecosystem name, returning an error if not found.
func (r *Registry) GetOrFail(ecosystem string) (*EcosystemAdapter, error) {
	adapter := r.Get(ecosystem)
	if adapter == nil {
		return nil, fmt.Errorf("no adapter registered for ecosystem %q", ecosystem)
	}
	return adapter, nil
}

// GetCommands returns command specs for a capability, or empty slice if not supported.
// Fails safely: unknown ecosystems and unsupported capabilities return empty slice.
func (r *Registry) GetCommands(ecosystem string, capability CapabilityType) []CommandSpec {
	adapter := r.Get(ecosystem)
	if adapter == nil {
		return []CommandSpec{}
	}
	cap := adapter.GetCapability(capability)
	if cap != nil && cap.Supported {
		return cap.Commands
	}
	return []CommandSpec{}
}

// ListAdapters returns all registered adapters.
func (r *Registry) ListAdapters() map[string]*EcosystemAdapter {
	result := make(map[string]*EcosystemAdapter)
	for k, v := range r.adapters {
		result[k] = v
	}
	return result
}

// ToJSON exports all adapters as JSON.
func (r *Registry) ToJSON() ([]byte, error) {
	adapters := make([]EcosystemAdapter, 0)
	for _, a := range r.adapters {
		adapters = append(adapters, *a)
	}
	return json.MarshalIndent(adapters, "", "  ")
}

// FromJSON loads adapters from JSON.
func (r *Registry) FromJSON(data []byte) error {
	var adapters []EcosystemAdapter
	if err := json.Unmarshal(data, &adapters); err != nil {
		return fmt.Errorf("failed to unmarshal adapters: %w", err)
	}
	for i := range adapters {
		if err := r.Register(&adapters[i]); err != nil {
			return err
		}
	}
	return nil
}

// BuildGoAdapter constructs the Go ecosystem adapter (MVP - full implementation).
func BuildGoAdapter() *EcosystemAdapter {
	return &EcosystemAdapter{
		Name:           "go",
		DisplayName:    "Go",
		PackageManager: "go mod",
		Capabilities: []EcosystemCapability{
			{
				Capability: CapabilityInstall,
				Supported:  true,
				Commands: []CommandSpec{
					{
						Cmd:         "go",
						Args:        []string{"mod", "download", "-x"},
						Description: "Download Go module dependencies",
					},
				},
			},
			{
				Capability: CapabilityBuild,
				Supported:  true,
				Commands: []CommandSpec{
					{
						Cmd:         "go",
						Args:        []string{"build", "-o", "/dev/null", "./..."},
						TimeoutSec:  300,
						Description: "Build all Go packages in module",
					},
				},
			},
			{
				Capability: CapabilityTest,
				Supported:  true,
				Commands: []CommandSpec{
					{
						Cmd:         "go",
						Args:        []string{"test", "-timeout", "5m", "-race", "./..."},
						TimeoutSec:  300,
						Description: "Run Go tests with race detector",
					},
				},
			},
			{
				Capability: CapabilityVet,
				Supported:  true,
				Commands: []CommandSpec{
					{
						Cmd:         "go",
						Args:        []string{"vet", "./..."},
						Description: "Run Go static analyzer",
					},
				},
			},
			{
				Capability: CapabilityAPIDiff,
				Supported:  false,
				Reason:     "Go API diff requires manual analysis or external tools (e.g., go-diff)",
			},
			{
				Capability: CapabilityReleaseNote,
				Supported:  false,
				Reason:     "Go release notes extracted from GitHub releases/CHANGELOG.md (framework-level)",
			},
		},
		FilePatterns: []string{"go.mod", "go.sum"},
		Metadata: map[string]interface{}{
			"version_file":   "go.mod",
			"version_format": "semantic",
		},
	}
}

// BuildNpmAdapter constructs the npm ecosystem adapter (placeholder - not implemented yet).
func BuildNpmAdapter() *EcosystemAdapter {
	return &EcosystemAdapter{
		Name:           "npm",
		DisplayName:    "npm",
		PackageManager: "npm",
		Capabilities: []EcosystemCapability{
			{Capability: CapabilityInstall, Supported: false, Reason: "npm adapter not yet implemented"},
			{Capability: CapabilityBuild, Supported: false, Reason: "npm adapter not yet implemented"},
			{Capability: CapabilityTest, Supported: false, Reason: "npm adapter not yet implemented"},
			{Capability: CapabilityVet, Supported: false, Reason: "npm adapter not yet implemented"},
			{Capability: CapabilityAPIDiff, Supported: false, Reason: "npm adapter not yet implemented"},
			{Capability: CapabilityReleaseNote, Supported: false, Reason: "npm adapter not yet implemented"},
		},
		FilePatterns: []string{"package.json", "package-lock.json", "yarn.lock"},
		Metadata: map[string]interface{}{
			"version_file":   "package.json",
			"version_format": "semantic",
		},
	}
}

// BuildPipAdapter constructs the pip ecosystem adapter (placeholder - not implemented yet).
func BuildPipAdapter() *EcosystemAdapter {
	return &EcosystemAdapter{
		Name:           "pip",
		DisplayName:    "Python",
		PackageManager: "pip",
		Capabilities: []EcosystemCapability{
			{Capability: CapabilityInstall, Supported: false, Reason: "pip adapter not yet implemented"},
			{Capability: CapabilityBuild, Supported: false, Reason: "pip adapter not yet implemented"},
			{Capability: CapabilityTest, Supported: false, Reason: "pip adapter not yet implemented"},
			{Capability: CapabilityVet, Supported: false, Reason: "pip adapter not yet implemented"},
			{Capability: CapabilityAPIDiff, Supported: false, Reason: "pip adapter not yet implemented"},
			{Capability: CapabilityReleaseNote, Supported: false, Reason: "pip adapter not yet implemented"},
		},
		FilePatterns: []string{"requirements.txt", "setup.py", "pyproject.toml"},
		Metadata: map[string]interface{}{
			"version_file":   "setup.py",
			"version_format": "semantic",
		},
	}
}

var defaultRegistry *Registry

// GetDefaultRegistry returns the default registry with built-in adapters.
func GetDefaultRegistry() *Registry {
	if defaultRegistry == nil {
		defaultRegistry = NewRegistry()
		_ = defaultRegistry.Register(BuildGoAdapter())
		_ = defaultRegistry.Register(BuildNpmAdapter())
		_ = defaultRegistry.Register(BuildPipAdapter())
	}
	return defaultRegistry
}
