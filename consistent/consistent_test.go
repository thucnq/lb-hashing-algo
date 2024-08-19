package consistent

import (
	"hash/fnv"
	"testing"
)

func newConfig() Config {
	return Config{
		PartitionCount:    23,
		ReplicationFactor: 20,
		Load:              1.25,
		HashFunc:          hashFunc{},
	}
}

type testMember string

func (tm testMember) String() string {
	return string(tm)
}

type hashFunc struct{}

func (hs hashFunc) Sum64(data []byte) uint64 {
	h := fnv.New64()
	h.Write(data)
	return h.Sum64()
}

func TestConsistentAdd(t *testing.T) {
}

func TestConsistentRemove(t *testing.T) {
}

func TestConsistentLoad(t *testing.T) {
}

func TestConsistentLocateKey(t *testing.T) {
}

func TestConsistentInsufficientMemberCount(t *testing.T) {
}

func TestConsistentClosestMembers(t *testing.T) {
}
