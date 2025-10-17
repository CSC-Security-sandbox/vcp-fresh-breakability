package rules

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions/processor"
)

func GetProxyRules() map[string]actions.Rule {
	return map[string]actions.Rule{
		"/api/storage/volumes": {
			GET: &processor.VolumeAction{
				Name: "Allow volume listing",
				RequestRule: actions.RequestRule{
					ValidationRules: []actions.ValidationRule{},
					InjectionRules:  []actions.InjectionRule{},
				},
				ResponseRule: actions.ResponseRule{
					InjectionRules: []actions.InjectionRule{},
					RemovalRules: []actions.RemovalRule{
						{FieldPath: "efficiency"},
						{FieldPath: "space.physical_used"},
					},
				},
			},
			POST: &processor.VolumeAction{
				Name: "Allow volume creation",
				RequestRule: actions.RequestRule{
					ValidationRules: []actions.ValidationRule{
						{
							FieldPath: "size",
							Required:  true,
						},
						{
							FieldPath: "name",
							Required:  true,
						},
						{
							FieldPath: "guarantee.type",
							Values:    []interface{}{"none"},
						},
						{
							FieldPath: "space.logical_space.enforcement",
							Values:    []interface{}{true},
						},
						{
							FieldPath: "space.logical_space.reporting",
							Values:    []interface{}{true},
						},
					},
					InjectionRules: []actions.InjectionRule{
						{
							FieldPath: "space.logical_space.enforcement",
							Value:     true,
						},
					},
				},
				ResponseRule: actions.ResponseRule{
					InjectionRules: []actions.InjectionRule{},
					RemovalRules: []actions.RemovalRule{
						{FieldPath: "efficiency"},
						{FieldPath: "space.physical_used"},
					},
				},
			},
		},
		"/api/storage/volumes/{uuid}": {
			GET: &processor.VolumeAction{
				Name: "Allow specific volume details",
				RequestRule: actions.RequestRule{
					ValidationRules: []actions.ValidationRule{},
					InjectionRules:  []actions.InjectionRule{},
				},
				ResponseRule: actions.ResponseRule{
					InjectionRules: []actions.InjectionRule{},
					RemovalRules: []actions.RemovalRule{
						{FieldPath: "efficiency"},
						{FieldPath: "space.physical_used"},
						{FieldPath: "space.logical_space.enforcement"},
						{FieldPath: "space.logical_space.reporting"},
					},
				},
			},
			PATCH: &processor.VolumeAction{
				Name: "Allow volume modification",
				RequestRule: actions.RequestRule{
					ValidationRules: []actions.ValidationRule{
						{
							FieldPath: "size",
							Required:  true,
						},
						{
							FieldPath: "name",
							Required:  true,
						},
						{
							FieldPath: "guarantee.type",
							Values:    []interface{}{"none", "volume"},
						},
						{
							FieldPath: "space.logical_space.enforcement",
							Values:    []interface{}{true},
						},
					},
					InjectionRules: []actions.InjectionRule{},
				},
				ResponseRule: actions.ResponseRule{
					InjectionRules: []actions.InjectionRule{},
					RemovalRules: []actions.RemovalRule{
						{FieldPath: "efficiency"},
						{FieldPath: "space.physical_used"},
					},
				},
			},
			DELETE: &processor.VolumeAction{
				Name: "Allow volume deletion",
				RequestRule: actions.RequestRule{
					ValidationRules: []actions.ValidationRule{},
					InjectionRules:  []actions.InjectionRule{},
				},
				ResponseRule: actions.ResponseRule{
					InjectionRules: []actions.InjectionRule{},
					RemovalRules: []actions.RemovalRule{
						{FieldPath: "efficiency"},
						{FieldPath: "space.physical_used"},
					},
				},
			},
		},
		"/api/storage/aggregates": {
			GET: &processor.Allow{
				Name: "Allow aggregate listing",
			},
			POST: &processor.Deny{
				Name: "Aggregate creation not allowed",
			},
			PATCH: &processor.Deny{
				Name: "Aggregate modification not allowed",
			},
			DELETE: &processor.Deny{
				Name: "Aggregate deletion not allowed",
			},
		},
	}
}
