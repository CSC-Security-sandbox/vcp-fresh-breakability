package models

type CacheParameters struct {
	PeerAddresses   []string
	PeerVolumeName  string
	PeerClusterName string
	PeerSvmName     string
	State           string
	StateDetails    string
	Command         string
}
