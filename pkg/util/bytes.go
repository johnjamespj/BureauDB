package util

import "crypto/md5"

func HashBytes(b []byte) []byte {
	res := md5.Sum(b)
	return res[:]
}

func Int64ToBytes(i int64) []byte {
	res := make([]byte, 8)
	for j := 0; j < 8; j++ {
		res[j] = byte(i >> uint(j*8))
	}
	return res
}

func BytesToInt64(b []byte, i int) int64 {
	res := int64(0)
	for j := 0; j < 8; j++ {
		res |= int64(b[i+j]) << uint(j*8)
	}
	return res
}
