package crypto

import (
	"crypto/rand"
	"errors"
)

func randomBytes(count int) ([]byte, error) {
	bytes := make([]byte, count)
	n, err := rand.Read(bytes)
	if err != nil {
		return nil, err
	}
	if n != count {
		return nil, errors.New("unexpected number of random bytes")
	}
	return bytes, nil
}
