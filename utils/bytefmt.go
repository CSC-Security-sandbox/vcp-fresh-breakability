package utils

import (
	"fmt"
)

const (
	k  = 1000
	m  = 1000 * k
	g  = 1000 * m
	t  = 1000 * g
	p  = 1000 * t
	ki = 1024
	mi = 1024 * ki
	gi = 1024 * mi
	ti = 1024 * gi
	pi = 1024 * ti
)

// FmtBytes formats a number of bytes into a human-readable string.
func FmtBytes(bytes int64) string {
	switch {
	case bytes == 0:
		return fmt.Sprintf("%dB", bytes)
	case bytes%p == 0:
		return fmt.Sprintf("%dPB", bytes/p)
	case bytes%t == 0:
		return fmt.Sprintf("%dTB", bytes/t)
	case bytes%g == 0:
		return fmt.Sprintf("%dGB", bytes/g)
	case bytes%m == 0:
		return fmt.Sprintf("%dMB", bytes/m)
	case bytes%k == 0:
		return fmt.Sprintf("%dkB", bytes/k)
	case bytes%pi == 0:
		return fmt.Sprintf("%dPiB", bytes/pi)
	case bytes%ti == 0:
		return fmt.Sprintf("%dTiB", bytes/ti)
	case bytes%gi == 0:
		return fmt.Sprintf("%dGiB", bytes/gi)
	case bytes%mi == 0:
		return fmt.Sprintf("%dMiB", bytes/mi)
	case bytes%ki == 0:
		return fmt.Sprintf("%dkiB", bytes/ki)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// FmtUint64Bytes formats a number of bytes into a human-readable string.
func FmtUint64Bytes(bytes uint64) string {
	switch {
	case bytes == 0:
		return fmt.Sprintf("%dB", bytes)
	case bytes%p == 0:
		return fmt.Sprintf("%dPB", bytes/p)
	case bytes%t == 0:
		return fmt.Sprintf("%dTB", bytes/t)
	case bytes%g == 0:
		return fmt.Sprintf("%dGB", bytes/g)
	case bytes%m == 0:
		return fmt.Sprintf("%dMB", bytes/m)
	case bytes%k == 0:
		return fmt.Sprintf("%dkB", bytes/k)
	case bytes%pi == 0:
		return fmt.Sprintf("%dPiB", bytes/pi)
	case bytes%ti == 0:
		return fmt.Sprintf("%dTiB", bytes/ti)
	case bytes%gi == 0:
		return fmt.Sprintf("%dGiB", bytes/gi)
	case bytes%mi == 0:
		return fmt.Sprintf("%dMiB", bytes/mi)
	case bytes%ki == 0:
		return fmt.Sprintf("%dkiB", bytes/ki)
	default:
		return fmt.Sprintf("%dB", bytes)
	}
}

// FmtUint64FlexGroupBytes formats a number of GigaBytes into a human-readable string.
func FmtUint64FlexGroupBytes(bytes uint64) string {
	return fmt.Sprintf("%.2fGiB", float64(bytes)/gi)
}
