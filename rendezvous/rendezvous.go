package rendezvous

type Rendezvous struct {
	nodes map[string]int
	nStr  []string
	nHash []uint64
	hash  HashFunc
}

type HashFunc func(s string) uint64

func New(nodes []string, hash HashFunc) *Rendezvous {
	r := &Rendezvous{
		nodes: make(map[string]int, len(nodes)),
		nStr:  make([]string, len(nodes)),
		nHash: make([]uint64, len(nodes)),
		hash:  hash,
	}

	for i, n := range nodes {
		r.nodes[n] = i
		r.nStr[i] = n
		r.nHash[i] = hash(n)
	}

	return r
}

func (r *Rendezvous) Lookup(k string) string {
	if len(r.nodes) == 0 {
		return ""
	}

	kHash := r.hash(k)

	var mIdx int
	var mHash = xorShiftMul64(kHash ^ r.nHash[0])

	for i, nHash := range r.nHash[1:] {
		if h := xorShiftMul64(kHash ^ nHash); h > mHash {
			mIdx = i + 1
			mHash = h
		}
	}

	return r.nStr[mIdx]
}

func (r *Rendezvous) Add(node string) {
	r.nodes[node] = len(r.nStr)
	r.nStr = append(r.nStr, node)
	r.nHash = append(r.nHash, r.hash(node))
}

func (r *Rendezvous) Remove(node string) {
	// get index of node to remove
	nIdx := r.nodes[node]

	// remove from the slices
	l := len(r.nStr)
	r.nStr[nIdx] = r.nStr[l]
	r.nStr = r.nStr[:l]

	r.nHash[nIdx] = r.nHash[l]
	r.nHash = r.nHash[:l]

	// update the map
	delete(r.nodes, node)
	moved := r.nStr[nIdx]
	r.nodes[moved] = nIdx
}

func xorShiftMul64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
