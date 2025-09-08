package models

import "time"

type CachePrePopulate struct {
	ExcludePathList []string `json:"excludePathList"`
	PathList        []string `json:"pathList"`
	Recursion       *bool    `json:"recursion"`
}
type CacheConfig struct {
	AtimeScrubEnabled       *bool  `json:"atimeScrubEnabled"`
	AtimeScrubDays          *int16 `json:"atimeScrubDays"`
	CifsChangeNotifyEnabled *bool  `json:"cifsChangeNotifyEnabled"`
	WritebackEnabled        *bool  `json:"writebackEnabled"`

	PrePopulate *CachePrePopulate `json:"prePopulate"`
}
type CacheParameters struct {
	PeerIPAddresses      []string     `json:"peerIPAddresses"`
	PeerClusterName      string       `json:"peerClusterName"`
	PeerSvmName          string       `json:"peerSvmName"`
	PeerVolumeName       string       `json:"peerVolumeName"`
	PeerExpiryTime       *time.Time   `json:"peerExpiryTime,omitempty"`
	PeeringCommand       string       `json:"peeringCommand,omitempty"`
	Passphrase           *string      `json:"passphrase,omitempty"`
	CacheConfig          *CacheConfig `json:"cacheConfig"`
	EnableGlobalFileLock *bool        `json:"enableGlobalFileLock,omitempty"`
}
