package consistent

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
)

// base on https://research.googleblog.com/2017/04/consistent-hashing-with-bounded-loads.html
// https://github.com/buraksezer/consistent

const (
	DefaultPartitionCount    int     = 271
	DefaultReplicationFactor int     = 20
	DefaultLoad              float64 = 1.25
)

var ErrInsufficientMemberCount = errors.New("insufficient member count")

type HashFunc interface {
	Sum64([]byte) uint64
}

type Member interface {
	String() string
}

type Config struct {
	HashFunc          HashFunc
	PartitionCount    int
	ReplicationFactor int
	Load              float64
}

type Consistent struct {
	mu sync.RWMutex

	config         Config
	hashFunc       HashFunc
	sortedSet      []uint64
	partitionCount uint64
	loads          map[string]float64
	members        map[string]*Member
	partitions     map[int]*Member
	ring           map[uint64]*Member
}

func New(members []Member, config Config) *Consistent {
	if config.HashFunc == nil {
		panic("HashFunc cannot be nil")
	}
	if config.PartitionCount == 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.Load == 0 {
		config.Load = DefaultLoad
	}

	c := &Consistent{
		config:         config,
		members:        make(map[string]*Member),
		partitionCount: uint64(config.PartitionCount),
		ring:           make(map[uint64]*Member),
	}

	c.hashFunc = config.HashFunc
	for _, member := range members {
		c.add(member)
	}
	if members != nil {
		c.distributePartitions()
	}
	return c
}

func (c *Consistent) GetMembers() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy of member list.
	members := make([]Member, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, *member)
	}
	return members
}

func (c *Consistent) AverageLoad() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.averageLoad()
}

func (c *Consistent) averageLoad() float64 {
	if len(c.members) == 0 {
		return 0
	}

	avgLoad := float64(c.partitionCount/uint64(len(c.members))) * c.config.Load
	return math.Ceil(avgLoad)
}

func (c *Consistent) distributeWithLoad(partID, idx int, partitions map[int]*Member, loads map[string]float64) {
	avgLoad := c.averageLoad()
	var count int
	for {
		count++
		if count >= len(c.sortedSet) {
			// User needs to decrease partition count, increase member count or increase load factor.
			panic("not enough room to distribute partitions")
		}
		i := c.sortedSet[idx]
		member := *c.ring[i]
		load := loads[member.String()]
		if load+1 <= avgLoad {
			partitions[partID] = &member
			loads[member.String()]++
			return
		}
		idx++
		if idx >= len(c.sortedSet) {
			idx = 0
		}
	}
}

func (c *Consistent) distributePartitions() {
	loads := make(map[string]float64)
	partitions := make(map[int]*Member)

	bs := make([]byte, 8)
	for partID := uint64(0); partID < c.partitionCount; partID++ {
		binary.LittleEndian.PutUint64(bs, partID)
		key := c.hashFunc.Sum64(bs)
		idx := sort.Search(len(c.sortedSet), func(i int) bool {
			return c.sortedSet[i] >= key
		})
		if idx >= len(c.sortedSet) {
			idx = 0
		}
		c.distributeWithLoad(int(partID), idx, partitions, loads)
	}
	c.partitions = partitions
	c.loads = loads
}

func (c *Consistent) add(member Member) {
	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", member.String(), i))
		h := c.hashFunc.Sum64(key)
		c.ring[h] = &member
		c.sortedSet = append(c.sortedSet, h)
	}
	// sort hashes ascendingly
	sort.Slice(c.sortedSet, func(i int, j int) bool {
		return c.sortedSet[i] < c.sortedSet[j]
	})
	// Storing member at this map is useful to find backup members of a partition.
	c.members[member.String()] = &member
}

// Add adds a new member to the consistent hash circle.
func (c *Consistent) Add(member Member) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[member.String()]; ok {
		// We already have this member. Quit immediately.
		return
	}
	c.add(member)
	c.distributePartitions()
}

func (c *Consistent) delSlice(val uint64) {
	for i := 0; i < len(c.sortedSet); i++ {
		if c.sortedSet[i] == val {
			c.sortedSet = append(c.sortedSet[:i], c.sortedSet[i+1:]...)
			break
		}
	}
}

func (c *Consistent) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[name]; !ok {
		// There is no member with that name. Quit immediately.
		return
	}

	for i := 0; i < c.config.ReplicationFactor; i++ {
		key := []byte(fmt.Sprintf("%s%d", name, i))
		h := c.hashFunc.Sum64(key)
		delete(c.ring, h)
		c.delSlice(h)
	}
	delete(c.members, name)
	if len(c.members) == 0 {
		// consistent hash ring is empty now. Reset the partition table.
		c.partitions = make(map[int]*Member)
		return
	}
	c.distributePartitions()
}

func (c *Consistent) LoadDistribution() map[string]float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a thread-safe copy
	res := make(map[string]float64)
	for member, load := range c.loads {
		res[member] = load
	}
	return res
}

func (c *Consistent) FindPartitionID(key []byte) int {
	hKey := c.hashFunc.Sum64(key)
	return int(hKey % c.partitionCount)
}

func (c *Consistent) GetPartitionOwner(partID int) Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.getPartitionOwner(partID)
}

func (c *Consistent) getPartitionOwner(partID int) Member {
	member, ok := c.partitions[partID]
	if !ok {
		return nil
	}
	// Create a thread-safe copy of member and return it.
	return *member
}

func (c *Consistent) LocateKey(key []byte) Member {
	partID := c.FindPartitionID(key)
	return c.GetPartitionOwner(partID)
}

func (c *Consistent) getClosestN(partID, count int) ([]Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var res []Member
	if count > len(c.members) {
		return res, ErrInsufficientMemberCount
	}

	var ownerKey uint64
	owner := c.getPartitionOwner(partID)
	// Hash and sort all the names.
	var keys []uint64
	kMems := make(map[uint64]*Member)
	for name, member := range c.members {
		key := c.hashFunc.Sum64([]byte(name))
		if name == owner.String() {
			ownerKey = key
		}
		keys = append(keys, key)
		kMems[key] = member
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Find the key owner
	idx := 0
	for idx < len(keys) {
		if keys[idx] == ownerKey {
			key := keys[idx]
			res = append(res, *kMems[key])
			break
		}
		idx++
	}

	// Find the closest(replica owners) members.
	for len(res) < count {
		idx++
		if idx >= len(keys) {
			idx = 0
		}
		key := keys[idx]
		res = append(res, *kMems[key])
	}
	return res, nil
}

func (c *Consistent) GetClosestN(key []byte, count int) ([]Member, error) {
	partID := c.FindPartitionID(key)
	return c.getClosestN(partID, count)
}

func (c *Consistent) GetClosestNForPartition(partID, count int) ([]Member, error) {
	return c.getClosestN(partID, count)
}
