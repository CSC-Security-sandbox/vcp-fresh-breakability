package utils

import (
	"crypto/rand"
	"encoding/hex"
)

// RandomHex10 returns a random string of format xxxxx-xxxxx (10 hex chars, split by dash)
func RandomHex10() string {
	b := make([]byte, 5)
	_, err := rand.Read(b)
	if err != nil {
		return "00000-00000"
	}
	h := hex.EncodeToString(b)
	return h[:5] + "-" + h[5:]
}
