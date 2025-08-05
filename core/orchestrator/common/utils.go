package common

const (
	LocalEnv = "local"
)

func CreateJunctionPath(token string) string {
	junctionPath := "/" + token
	return junctionPath
}
