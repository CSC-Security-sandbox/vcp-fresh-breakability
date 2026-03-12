package rules_v2

import (
	. "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
)

// GetProxyRules returns the rule map for the proxy.
// This uses the DSL-based rule engine for cleaner rule definitions.
func GetProxyRules() map[string]Rule {
	return map[string]Rule{
		// Storage Volumes - list and create
		"/api/storage/volumes": {
			GET: Allow{
				Name: "Allow volume listing",
				ModifyResponse: RemoveFields{
					Fields: []string{
						"$.efficiency",
						"$.space.physical_used",
						"$.space.physical_used_percent",
						"$.space.footprint",
						"$.space.total_footprint",
						"$.space.local_tier_footprint",
						"$.space.capacity_tier_footprint",
						"$.space.performance_tier_footprint",
						"$.space.effective_total_footprint",
						"$.space.delayed_free_footprint",
						"$.space.volume_guarantee_footprint",
						"$.space.snapmirror_destination_footprint",
						"$.space.file_operation_metadata",
						"$.space.metadata",
						"$.space.total_metadata",
						"$.space.total_metadata_footprint",
						"$.space.dedupe_metafiles_footprint",
						"$.space.dedupe_metafiles_temporary_footprint",
						"$.space.cross_volume_dedupe_metafiles_footprint",
						"$.space.cross_volume_dedupe_metafiles_temporary_footprint",
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
							"$.space.physical_used_percent",
							"$.space.footprint",
							"$.space.total_footprint",
							"$.space.local_tier_footprint",
							"$.space.capacity_tier_footprint",
							"$.space.performance_tier_footprint",
							"$.space.effective_total_footprint",
							"$.space.delayed_free_footprint",
							"$.space.volume_guarantee_footprint",
							"$.space.snapmirror_destination_footprint",
							"$.space.file_operation_metadata",
							"$.space.metadata",
							"$.space.total_metadata",
							"$.space.total_metadata_footprint",
							"$.space.dedupe_metafiles_footprint",
							"$.space.dedupe_metafiles_temporary_footprint",
							"$.space.cross_volume_dedupe_metafiles_footprint",
							"$.space.cross_volume_dedupe_metafiles_temporary_footprint",
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
						"$.space.physical_used_percent",
						"$.space.footprint",
						"$.space.total_footprint",
						"$.space.local_tier_footprint",
						"$.space.capacity_tier_footprint",
						"$.space.performance_tier_footprint",
						"$.space.effective_total_footprint",
						"$.space.delayed_free_footprint",
						"$.space.volume_guarantee_footprint",
						"$.space.snapmirror_destination_footprint",
						"$.space.file_operation_metadata",
						"$.space.metadata",
						"$.space.total_metadata",
						"$.space.total_metadata_footprint",
						"$.space.dedupe_metafiles_footprint",
						"$.space.dedupe_metafiles_temporary_footprint",
						"$.space.cross_volume_dedupe_metafiles_footprint",
						"$.space.cross_volume_dedupe_metafiles_temporary_footprint",
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
							"$.space.physical_used_percent",
							"$.space.footprint",
							"$.space.total_footprint",
							"$.space.local_tier_footprint",
							"$.space.capacity_tier_footprint",
							"$.space.performance_tier_footprint",
							"$.space.effective_total_footprint",
							"$.space.delayed_free_footprint",
							"$.space.volume_guarantee_footprint",
							"$.space.snapmirror_destination_footprint",
							"$.space.file_operation_metadata",
							"$.space.metadata",
							"$.space.total_metadata",
							"$.space.total_metadata_footprint",
							"$.space.dedupe_metafiles_footprint",
							"$.space.dedupe_metafiles_temporary_footprint",
							"$.space.cross_volume_dedupe_metafiles_footprint",
							"$.space.cross_volume_dedupe_metafiles_temporary_footprint",
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
							"$.space.physical_used_percent",
							"$.space.footprint",
							"$.space.total_footprint",
							"$.space.local_tier_footprint",
							"$.space.capacity_tier_footprint",
							"$.space.performance_tier_footprint",
							"$.space.effective_total_footprint",
							"$.space.delayed_free_footprint",
							"$.space.volume_guarantee_footprint",
							"$.space.snapmirror_destination_footprint",
							"$.space.file_operation_metadata",
							"$.space.metadata",
							"$.space.total_metadata",
							"$.space.total_metadata_footprint",
							"$.space.dedupe_metafiles_footprint",
							"$.space.dedupe_metafiles_temporary_footprint",
							"$.space.cross_volume_dedupe_metafiles_footprint",
							"$.space.cross_volume_dedupe_metafiles_temporary_footprint",
						},
					},
				},
			},
		},

		// Private CLI Volume rename - PATCH with query vserver, volume and body newname
		"/api/private/cli/volume/rename": {
			GET:  DenyAll{},
			POST: DenyAll{},
			PATCH: When{
				Name: "Private CLI volume rename validation",
				Condition: And(
					HasFields("newname"),
					validatePrivateCLIVolumeRename,
				),
				IsTrue: Allow{
					Name: "Allow private CLI volume rename",
				},
			},
			DELETE: DenyAll{},
		},

		// Private CLI Volume show-footprint - denied (exposes footprint data)
		"/api/private/cli/volume/show-footprint": {
			GET:    Deny{Name: "not allowed"},
			POST:   DenyAll{},
			PATCH:  DenyAll{},
			DELETE: DenyAll{},
		},

		// Private CLI Volume - same controls as standard volume CRUD
		// Must be defined before /api/private/* for exact match to take precedence
		"/api/private/cli/volume": {
			GET: Allow{
				Name: "Allow private CLI volume listing",
				ModifyResponse: RemoveFields{
					Fields: []string{
						"$.percent_used",
						"$.physical_used",
						"$.physical_used_percent",
						"$.total_metadata",
						"$.total_metadata_footprint",
						"$.space_guarantee",
						"$.space_guarantee_enabled",
						"$.space_slo",
						"$.is_space_slo_enabled",
						"$.sis_space_saved",
						"$.sis_space_saved_percent",
						"$.dedupe_space_saved",
						"$.dedupe_space_saved_percent",
						"$.dedupe_space_shared",
						"$.compression_space_saved",
						"$.compression_space_saved_percent",
					},
				},
			},
			POST: When{
				Name: "Private CLI volume creation validation",
				Condition: And(
					HasFields("size", "volume", "vserver"),
					IfPresentThenValue("space_guarantee", "none"),
					IfPresentThenEquals("is_space_enforcement_logical", true),
					IfPresentThenEquals("is_space_reporting_logical", true),
					validatePrivateCLIVolumeCreation,
				),
				IsTrue: Allow{
					Name: "Allow private CLI volume creation",
					ModifyRequest: SetRequestFields{
						Fields: map[string]interface{}{
							"is_space_enforcement_logical": true,
							"is_space_reporting_logical":   true,
						},
					},
				},
			},
			PATCH: When{
				Name: "Private CLI volume modification validation",
				Condition: And(
					IfPresentThenValue("space_guarantee", "none"),
					IfPresentThenEquals("is_space_enforcement_logical", true),
					validatePrivateCLIVolumeModification,
				),
				IsTrue: Allow{
					Name: "Allow private CLI volume modification",
				},
			},
			DELETE: When{
				Name:      "Private CLI volume deletion validation",
				Condition: validatePrivateCLIVolumeDeletion,
				IsTrue: Allow{
					Name: "Allow private CLI volume deletion",
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

		// SnapLock event retention (EBR) policies - bulk delete and bulk update not allowed
		"/api/storage/snaplock/event-retention/policies": {
			DELETE: Deny{Name: "Bulk delete of event retention policies is not allowed"},
			PATCH:  Deny{Name: "Bulk update of event retention policies is not allowed"},
		},

		// Cluster counter tables - block all (performance/diagnostic data; ONTAP REST counter_table_collection_get, counter_table_get, rows)
		"/api/cluster/counter/tables/*": {
			GET:    Deny{Name: "Cluster counter tables not allowed"},
			POST:   DenyAll{},
			PATCH:  DenyAll{},
			DELETE: DenyAll{},
		},
	}
}
