package utils

import (
	"math"
	"testing"
)

var byteFmtTests = []struct {
	in  int64
	out string
}{
	{500000000000, "500GB"},
	{15000000000000, "15TB"},
	{1, "1B"},
	{1000, "1kB"},
	{1000000, "1MB"},
	{1000000000, "1GB"},
	{1000000000000, "1TB"},
	{1000000000000000, "1PB"},
	{1001, "1001B"},
	{1001000, "1001kB"},
	{1001000000, "1001MB"},
	{1001000000000, "1001GB"},
	{1001000000000000, "1001TB"},
	{1001000000000000000, "1001PB"},
	{math.MinInt64 + 1, "-9223372036854775807B"},
	{-9223000000000000000, "-9223PB"},
	{-1, "-1B"},
	{-0, "0B"},
	{0, "0B"},
	{1, "1B"},
	{9223000000000000000, "9223PB"},
	{math.MaxInt64, "9223372036854775807B"},
}

var bibFmtTests = []struct {
	in  int64
	out string
}{
	{53687091200, "50GiB"},
	{16492674416640, "15TiB"},
	{1, "1B"},
	{1024, "1kiB"},
	{1048576, "1MiB"},
	{1073741824, "1GiB"},
	{1099511627776, "1TiB"},
	{1125899906842624, "1PiB"},
	{1001, "1001B"},
	{1025024, "1001kiB"},
	{1049624576, "1001MiB"},
	{1074815565824, "1001GiB"},
	{1100611139403776, "1001TiB"},
	{1127025806749466624, "1001PiB"},
	{math.MinInt64, "-8192PiB"},
	{-8132375027124273152, "-7223PiB"},
	{8132375027124273152, "7223PiB"},
	{math.MaxInt64, "9223372036854775807B"},
}

func TestFmtBibs(t *testing.T) {
	for _, tt := range bibFmtTests {
		t.Run(tt.out, func(t *testing.T) {
			s := FmtBytes(tt.in)
			if s != tt.out {
				t.Errorf("got %q, want %q", s, tt.out)
			}
		})
	}
}

func TestFmtBytes(t *testing.T) {
	for _, tt := range byteFmtTests {
		t.Run(tt.out, func(t *testing.T) {
			s := FmtBytes(tt.in)
			if s != tt.out {
				t.Errorf("got %q, want %q", s, tt.out)
			}
		})
	}
}

var byteFmtUint64Tests = []struct {
	in  uint64
	out string
}{
	{500000000000, "500GB"},
	{15000000000000, "15TB"},
	{1, "1B"},
	{1000, "1kB"},
	{1000000, "1MB"},
	{1000000000, "1GB"},
	{1000000000000, "1TB"},
	{1000000000000000, "1PB"},
	{1001, "1001B"},
	{1001000, "1001kB"},
	{1001000000, "1001MB"},
	{1001000000000, "1001GB"},
	{1001000000000000, "1001TB"},
	{1001000000000000000, "1001PB"},
	{0, "0B"},
	{1, "1B"},
	{9223000000000000000, "9223PB"},
	{math.MaxUint64, "18446744073709551615B"},
}

var bibFmtUint64Tests = []struct {
	in  uint64
	out string
}{
	{53687091200, "50GiB"},
	{16492674416640, "15TiB"},
	{1, "1B"},
	{1024, "1kiB"},
	{1048576, "1MiB"},
	{1073741824, "1GiB"},
	{1099511627776, "1TiB"},
	{1125899906842624, "1PiB"},
	{1001, "1001B"},
	{1025024, "1001kiB"},
	{1049624576, "1001MiB"},
	{1074815565824, "1001GiB"},
	{1100611139403776, "1001TiB"},
	{1127025806749466624, "1001PiB"},
	{8132375027124273152, "7223PiB"},
	{math.MaxUint64, "18446744073709551615B"},
}

func TestFmtUint64Bibs(t *testing.T) {
	for _, tt := range bibFmtUint64Tests {
		t.Run(tt.out, func(t *testing.T) {
			s := FmtUint64Bytes(tt.in)
			if s != tt.out {
				t.Errorf("got %q, want %q", s, tt.out)
			}
		})
	}
}

func TestFmtUint64Bytes(t *testing.T) {
	for _, tt := range byteFmtUint64Tests {
		t.Run(tt.out, func(t *testing.T) {
			s := FmtUint64Bytes(tt.in)
			if s != tt.out {
				t.Errorf("got %q, want %q", s, tt.out)
			}
		})
	}
}

var byteFmtFlexGroupVolTestsUint64 = []struct {
	in  uint64
	out string
}{
	{1, "0.00GiB"},
	{1001, "0.00GiB"},
	{math.MaxInt64, "8589934592.00GiB"},
}

func TestFmtUint64FlexGroupBytes(t *testing.T) {
	for _, tt := range byteFmtFlexGroupVolTestsUint64 {
		t.Run(tt.out, func(t *testing.T) {
			s := FmtUint64FlexGroupBytes(tt.in)
			if s != tt.out {
				t.Errorf("got %q, want %q", s, tt.out)
			}
		})
	}
}
