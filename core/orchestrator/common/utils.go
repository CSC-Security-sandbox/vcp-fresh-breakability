package common

import (
	"regexp"
)

var (
	SnapmirrorSnapshotPrefix = regexp.MustCompile("^snapmirror.*$")
)

func CreateJunctionPath(token string) string {
	junctionPath := "/" + token
	return junctionPath
}
