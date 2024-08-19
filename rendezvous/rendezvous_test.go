package rendezvous

import (
	"hash/fnv"
	"testing"
)

func hashFunc(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func TestLookup(t *testing.T) {
	type args struct {
		nodes []string
		hash  HashFunc
		k     string
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal case",
			args: args{
				nodes: []string{},
				hash:  hashFunc,
				k:     "Hello World!",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(tt.args.nodes, tt.args.hash)
			got.Lookup(tt.args.k)
		})
	}
}
