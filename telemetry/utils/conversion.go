package utils

import "math/big"

const MibToBytesFactor = 1024 * 1024
const MibPerGib = 1024
const KibToBytesFactor = 1024
const KibPerGib = 1024 * 1024

func MibToBytes(mibHours float64) int64 {
	result := mibHours * MibToBytesFactor
	return int64(result)
}

func MibHoursToGibHours(mibHours float64) int64 {
	result := mibHours / MibPerGib
	return int64(result)
}

func MibHoursToGibHoursWithRoundOff(mibHours float64) int64 {
	scaled := new(big.Float).SetFloat64(mibHours)
	scaled = new(big.Float).SetPrec(5).SetMode(big.ToNearestEven).Set(scaled)
	result := new(big.Float).Quo(scaled, big.NewFloat(MibPerGib))
	resultInt, _ := result.Int64()
	return resultInt
}

func MibtoKib(mib float64) int64 {
	result := mib * 1024
	return int64(result)
}
