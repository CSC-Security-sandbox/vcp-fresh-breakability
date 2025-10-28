package helper

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

func GeneratePasswordSecretId(secretManagerProjectID string, accountID string, adName string, region string) string {
	data := fmt.Sprintf("%s-%s-%s-%s", secretManagerProjectID, accountID, adName, region)
	hash := sha256.Sum256([]byte(data))
	return "gcnv-" + hex.EncodeToString(hash[:8])[:15]
}
