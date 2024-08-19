package jump

import (
	"fmt"
	"hash"
	"strconv"
	"testing"
)

var jumpTestVectors = []struct {
	key      uint64
	buckets  int32
	expected int32
}{
	{1, 1, 0},
	{42, 57, 43},
	{0xDEAD10CC, 1, 0},
	{0xDEAD10CC, 666, 361},
	{256, 1024, 520},
	// Test negative values
	{0, -10, 0},
	{0xDEAD10CC, -666, 0},
}

func TestJumpHash(t *testing.T) {
	for _, v := range jumpTestVectors {
		h := Hash(v.key, v.buckets)
		if h != v.expected {
			t.Errorf("expected bucket for key=%d to be %d, got %d",
				v.key, v.expected, h)
		}
	}
}

var jumpStringTestVectors = []struct {
	key      string
	buckets  int32
	hashFunc func() hash.Hash64
	expected int32
}{
	{"localhost", 10, NewCRC32, 9},
	{"ёлка", 10, NewCRC64, 6},
	{"ветер", 10, NewFNV1, 3},
	{"中国", 10, NewFNV1a, 5},
	{"日本", 10, NewCRC64, 6},
}

func TestJumpHashString(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		h := HashString(v.key, v.buckets, v.hashFunc())
		if h != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func TestHashFunc(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		hashFunc := New(int(v.buckets), v.hashFunc())
		h := hashFunc.Hash(v.key)
		if int32(h) != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func ExampleHash() {
	fmt.Print(Hash(256, 1024))
	// Output: 520
}
