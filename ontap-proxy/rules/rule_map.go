package rules

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"

func GetProxyRules() map[string]actions.Rule {
	return map[string]actions.Rule{
		"/api/storage/qtrees": {
			GET: actions.Allow{
				Name: "Allow qtree listing",
			},
			POST: actions.Allow{
				Name: "Allow qtree creation",
			},
			PATCH: actions.Allow{
				Name: "Allow qtree modification",
			},
			DELETE: actions.Allow{
				Name: "Allow qtree deletion",
			},
		},
		"/api/storage/aggregates": {
			GET: actions.Allow{
				Name: "Allow aggregate listing",
			},
			POST:   actions.DenyAll(),
			PATCH:  actions.DenyAll(),
			DELETE: actions.DenyAll(),
		},
		"/api/storage/volumes": {
			GET: actions.Allow{
				Name:         "Allow volume listing",
				RemoveFields: []string{"aggregates"},
			},
			POST:   actions.DenyAll(),
			PATCH:  actions.DenyAll(),
			DELETE: actions.DenyAll(),
		},
		"/api/storage/volumes/{uuid}": {
			GET: actions.Allow{
				Name: "Allow specific volume details",
			},
			POST:   actions.DenyAll(),
			PATCH:  actions.DenyAll(),
			DELETE: actions.DenyAll(),
		},
	}
}
