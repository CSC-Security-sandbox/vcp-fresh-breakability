package utils

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
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

const (
	_  = iota
	KB = 1_000
	MB = KB * 1_000
	GB = MB * 1_000
	TB = GB * 1_000
	PB = TB * 1_000
)

var unitMultipliers = map[string]Unit{
	"B":   1,
	"KB":  KB,
	"MB":  MB,
	"GB":  GB,
	"TB":  TB,
	"PB":  PB,
	"KIB": KiB,
	"MIB": MiB,
	"GIB": GiB,
	"TIB": TiB,
	"PIB": PiB,
	"KI":  KiB,
	"MI":  MiB,
	"GI":  GiB,
	"TI":  TiB,
	"PI":  PiB,
}

func ParseUintSizeToBytes(input string) (uint64, error) {
	input = strings.TrimSpace(input)

	// Find where the numeric part ends
	var i int
	startIndex := 0
	for i = range len(input) {
		r := rune(input[i])
		if i == 0 && (r == '-' || r == '+') {
			startIndex = 1
			continue
		}
		if !unicode.IsDigit(r) && r != '.' {
			break
		}
	}

	if i == 0 {
		return 0, fmt.Errorf("no numeric value found")
	}
	numberStr := input[startIndex:i]
	unitStr := strings.ToUpper(strings.TrimSpace(input[i:]))

	// Parse the numeric part as uint64
	value, err := strconv.ParseUint(numberStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number: %v", err)
	}

	// Get the multiplier
	multiplier, ok := unitMultipliers[unitStr]
	if !ok {
		return 0, fmt.Errorf("unknown unit: %s", unitStr)
	}

	result := value * uint64(multiplier)
	return result, nil
}

func ParseIntSizeToBytes(input string) (int64, error) {
	signMultiplier := int64(1)
	if strings.HasPrefix(input, "-") {
		signMultiplier = -1
	}
	value, err := ParseUintSizeToBytes(strings.TrimPrefix(input, "-"))
	if err != nil {
		return 0, fmt.Errorf("failed to parse size: %v", err)
	}

	result := signMultiplier * int64(value)
	return result, nil
}
