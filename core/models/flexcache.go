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
	PeerClusterName      string       `json:"peerClusterName"`
	PeerSvmName          string       `json:"peerSvmName"`
	PeerVolumeName       string       `json:"peerVolumeName"`
	PeerIPAddresses      []string     `json:"peerIPAddresses"`
	EnableGlobalFileLock *bool        `json:"enableGlobalFileLock,omitempty"`
	CacheConfig          *CacheConfig `json:"cacheConfig"`

	CacheState            string `json:"cache_state"`
	PreviousCacheState    string `json:"previous_cache_state"`
	CacheStateDetails     string `json:"cache_state_details,omitempty"`
	CacheStateDetailsCode int    `json:"cache_state_details_code,omitempty"`

	PeerExpiryTime *time.Time `json:"peerExpiryTime,omitempty"`
	PeeringCommand string     `json:"peeringCommand,omitempty"`
	Passphrase     *string    `json:"passphrase,omitempty"`
}
