package crypto

var (
	// V1 is the first version supported by qstackcrypto
	V1 = &Version{Name: "V1", Algorithm: "AES", Bits: 256, Mode: "GCM", KeyDerivationAlgorithm: "PBKDF2WithHmacSHA256", Iterations: 100000}
)

// Version defines a version of cryptography used by qstackcrypto
type Version struct {
	Name                   string
	Algorithm              string
	Bits                   int
	Mode                   string
	KeyDerivationAlgorithm string
	Iterations             int
}
