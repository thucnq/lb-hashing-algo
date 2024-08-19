package jump

import (
	"hash"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"io"
)

// base on https://arxiv.org/pdf/1406.2294v1
// https://github.com/lithammer/go-jump-consistent-hash

func Hash(key uint64, buckets int32) int32 {
	var b, j int64

	if buckets <= 0 {
		buckets = 1
	}

	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

func HashString(key string, buckets int32, h KeyHashFunc) int32 {
	h.Reset()
	_, err := io.WriteString(h, key)
	if err != nil {
		panic(err)
	}
	return Hash(h.Sum64(), buckets)
}

type KeyHashFunc interface {
	io.Writer

	Reset()
	Sum64() uint64
}

type HashFunc struct {
	n int32
	h KeyHashFunc
}

func New(n int, h KeyHashFunc) *HashFunc {
	return &HashFunc{int32(n), h}
}

func (h *HashFunc) N() int {
	return int(h.n)
}

func (h *HashFunc) Hash(key string) int {
	return int(HashString(key, h.n, h.h))
}

var (
	NewCRC32 func() hash.Hash64 = func() hash.Hash64 { return &crc32HashFunc{crc32.NewIEEE()} }
	NewCRC64 func() hash.Hash64 = func() hash.Hash64 { return crc64.New(crc64.MakeTable(crc64.ECMA)) }
	NewFNV1  func() hash.Hash64 = func() hash.Hash64 { return fnv.New64() }
	NewFNV1a func() hash.Hash64 = func() hash.Hash64 { return fnv.New64a() }

	CRC32 hash.Hash64 = &crc32HashFunc{crc32.NewIEEE()}
	CRC64 hash.Hash64 = crc64.New(crc64.MakeTable(crc64.ECMA))
	FNV1  hash.Hash64 = fnv.New64()
	FNV1a hash.Hash64 = fnv.New64a()
)
