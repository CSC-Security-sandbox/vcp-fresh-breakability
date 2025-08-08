package common

import (
	"regexp"
)
const (
	LocalEnv = "local"
)

var (
	SnapmirrorSnapshotPrefix = regexp.MustCompile("^snapmirror.*$")
)

func CreateJunctionPath(token string) string {
	junctionPath := "/" + token
	return junctionPath
}
