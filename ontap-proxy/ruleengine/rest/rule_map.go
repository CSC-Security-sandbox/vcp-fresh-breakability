package rules_v2

import (
	. "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
)

// GetProxyRules returns the rule map for the proxy.
// This uses the DSL-based rule engine for cleaner rule definitions.
func GetProxyRules() map[string]Rule {
	return map[string]Rule{
		// Private API - deny all access
		"/api/private/*": {
			GET:    Deny{Name: "Private API access denied"},
			POST:   Deny{Name: "Private API access denied"},
			PATCH:  Deny{Name: "Private API access denied"},
			DELETE: Deny{Name: "Private API access denied"},
		},

		// Storage Volumes - list and create
		"/api/storage/volumes": {
			GET: Allow{
				Name: "Allow volume listing",
				ModifyResponse: RemoveFields{
					Fields: []string{
						"$.efficiency",
						"$.space.physical_used",
						"$.space.logical_space.enforcement",
						"$.space.logical_space.reporting",
					},
				},
			},
			POST: When{
				Name: "Volume creation validation",
				// All conditions return (bool, reason) - And() returns the first failure reason
				Condition: And(
					HasFields("size", "name"),
					IfPresentThenValue("guarantee.type", "none"),
					IfPresentThenValue("snaplock.type", "enterprise", "non_snaplock"),
					IfPresentThenEquals("space.logical_space.enforcement", true),
					IfPresentThenEquals("space.logical_space.reporting", true),
					validateVolumeCreation, // Returns specific error from core API
				),
				// No IsFalse needed - uses condition's reason directly
				IsTrue: Allow{
					Name: "Allow volume creation",
					ModifyRequest: SetRequestFields{
						Fields: map[string]interface{}{
							"space.logical_space.enforcement": true,
							"space.logical_space.reporting":   true,
						},
					},
					ModifyResponse: RemoveFields{
						Fields: []string{
							"$.efficiency",
							"$.space.physical_used",
						},
					},
				},
			},
			PATCH:  DenyAll{},
			DELETE: DenyAll{},
		},

		// Storage Volumes - specific volume operations
		"/api/storage/volumes/{uuid}": {
			GET: Allow{
				Name: "Allow specific volume details",
				ModifyResponse: RemoveFields{
					Fields: []string{
						"$.efficiency",
						"$.space.physical_used",
						"$.space.logical_space.enforcement",
						"$.space.logical_space.reporting",
					},
				},
			},
			POST: DenyAll{},
			PATCH: When{
				Name: "Volume modification validation",
				Condition: And(
					IfPresentThenValue("guarantee.type", "none"),
					IfPresentThenValue("snaplock.type", "enterprise", "non_snaplock"),
					IfPresentThenEquals("space.logical_space.enforcement", true),
					validateVolumeModification,
				),
				// No IsFalse needed - uses condition's reason directly
				IsTrue: Allow{
					Name: "Allow volume modification",
					ModifyResponse: RemoveFields{
						Fields: []string{
							"$.efficiency",
							"$.space.physical_used",
						},
					},
				},
			},
			DELETE: When{
				Name:      "Volume deletion validation",
				Condition: validateVolumeDeletion,
				IsTrue: Allow{
					Name: "Allow volume deletion",
					ModifyResponse: RemoveFields{
						Fields: []string{
							"$.efficiency",
							"$.space.physical_used",
						},
					},
				},
			},
		},

		// Storage Aggregates
		"/api/storage/aggregates": {
			GET:    Allow{Name: "Allow aggregate listing"},
			POST:   Deny{Name: "Aggregate creation not allowed"},
			PATCH:  Deny{Name: "Aggregate modification not allowed"},
			DELETE: Deny{Name: "Aggregate deletion not allowed"},
		},

		// Storage Aggregates - specific aggregate
		"/api/storage/aggregates/{uuid}": {
			GET:    Allow{Name: "Allow aggregate details"},
			POST:   DenyAll{},
			PATCH:  Deny{Name: "Aggregate modification not allowed"},
			DELETE: Deny{Name: "Aggregate deletion not allowed"},
		},

		// Security Certificates
		"/api/security/certificates": {
			GET:    AllowAll{},
			POST:   AllowAll{},
			PATCH:  Deny{Name: "Certificate collection modification not allowed"},
			DELETE: Deny{Name: "Certificate deletion not allowed"},
		},

		// Security Certificates - specific certificate
		"/api/security/certificates/{uuid}": {
			GET:    AllowAll{},
			POST:   DenyAll{},
			PATCH:  AllowAll{},
			DELETE: Deny{Name: "Certificate deletion not allowed"},
		},
	}
}
