package flexcache

type Action string

const (
	ActionReady  Action = "ready"
	ActionCreate Action = "create"
	ActionWait   Action = "wait"
)
