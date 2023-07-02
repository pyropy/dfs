package checksum

import "crypto/sha256"

func CalculateCheckSum(data []byte) int {
	result := 0
	bytes := sha256.Sum256(data)

	for i := 0; i < 4; i++ {
		result = result << 8
		result += int(bytes[i])

	}

	return result
}
