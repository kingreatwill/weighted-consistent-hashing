package v2

import (
	"hash/crc32"
	"sort"
	"sync"
)

// DefaultWeightMultiplier is default value to use for Ring.WeightMultiplier if
// it is zero.
const DefaultWeightMultiplier uint32 = 100

// Ring is a consistent-hashing ring.
//
// It is a set-like collection that allows efficient, consisting mapping of
// arbitrary keys to the members of the set with minimum redistribution of these
// mappings when set membership is changed.
type Ring struct {
	// WeightMultiplier is the number that each member's weight is multiplied by
	// to produce the number of entries to add to the ring for each member.
	// If it zero DefaultWeightMultiplier is used.
	WeightMultiplier uint32

	m       sync.RWMutex
	nodes   []node
	members map[string]*member
}

// Add adds a member to the ring.
//
// m uniquely identifies the member.
//
// k is the value that is hashed to determine where the member is placed within
// the ring.
//
// w is the member's relative weight, which controls the proportion of the ring
// that is assigned to this member. A member with a weight of 2 occupies twice
// the space on the ring as a member with a weight of 1.
//
// It returns false is m is already a member of the ring.
func (d *Ring) Add(m string, k []byte, w uint32) bool {
	d.m.Lock()
	defer d.m.Unlock()

	if _, ok := d.members[m]; ok {
		return false
	}

	wm := d.WeightMultiplier
	if wm == 0 {
		wm = DefaultWeightMultiplier
	}

	mem := &member{m, k, w * wm}

	d.each(
		mem,
		func(h uint32) {
			i := d.find(h, false)

			if i < len(d.nodes) {
				node := &d.nodes[i]

				if node.Hash == h {
					// we found an existing node with this exact hash
					node.Add(mem)
					return
				}
			}

			// otherwise we need to insert a new node
			d.nodes = append(d.nodes, node{}) // grow by 1
			copy(d.nodes[i+1:], d.nodes[i:])  // move everything after i back
			d.nodes[i] = node{                // replace i
				h,
				[]*member{mem},
			}
		},
	)

	if d.members == nil {
		d.members = map[string]*member{}
	}

	d.members[m] = mem

	return true
}

// Remove removes a member from the ring.
//
// It returns false if m is not a member of the ring.
func (d *Ring) Remove(m string) bool {
	d.m.Lock()
	defer d.m.Unlock()

	mem, ok := d.members[m]
	if !ok {
		return false
	}

	d.each(
		mem,
		func(h uint32) {
			i := d.find(h, false)

			if i < len(d.nodes) {
				node := &d.nodes[i]
				if node.Hash == h {
					// we found an existing node with this exact hash
					node.Remove(mem)

					// if there are no members left in the node remove it entirely
					if len(node.Members) == 0 {
						d.nodes = append(d.nodes[:i], d.nodes[i+1:]...)
					}
				}
			}
		},
	)

	delete(d.members, m)

	return true
}

// Get returns the member from the ring that k maps.
func (d *Ring) Get(k []byte) (m string, ok bool) {
	h := crc32.ChecksumIEEE(k)

	d.m.RLock()
	defer d.m.RUnlock()

	i := d.find(h, true)

	if i < len(d.nodes) {
		return d.nodes[i].Members[0].ID, true
	}

	return "", false
}

// Ordered returns all members on the ring, ordered according to their distance
// from k.
//
// The first element is the same as the value returned by Get(). Each subsequent
// element is the member that would have been returned by Get() had the element
// before it had not been a member of the ring.
func (d *Ring) Ordered(k []byte) []string {
	h := crc32.ChecksumIEEE(k)

	d.m.RLock()
	defer d.m.RUnlock()

	var (
		bisect  = d.find(h, true)
		visited = map[string]struct{}{}
		members = make([]string, 0, len(d.members))
	)

	for i := bisect; i < len(d.nodes); i++ {
		for _, m := range d.nodes[i].Members {
			if _, ok := visited[m.ID]; ok {
				continue
			}

			members = append(members, m.ID)
			visited[m.ID] = struct{}{}

			if len(members) == len(d.members) {
				return members
			}
		}
	}

	for i := 0; i < bisect; i++ {
		for _, m := range d.nodes[i].Members {
			if _, ok := visited[m.ID]; ok {
				continue
			}

			members = append(members, m.ID)
			visited[m.ID] = struct{}{}

			if len(members) == len(d.members) {
				return members
			}
		}
	}

	return members
}

// each calls fn(hash) for each of the hashes produced from m.
func (d *Ring) each(m *member, fn func(uint32)) {
	h := crc32.NewIEEE()
	h.Write([]byte(m.Key))

	// mutate the hash by writing a deterministic nonce for each node
	nonce := []byte{0}

	for n := uint32(0); n < m.Weight; n++ {
		fn(h.Sum32())
		h.Write(nonce)
		nonce[0]++
	}
}

// find returns the index of the first node with a hash >= h.
func (d *Ring) find(h uint32, wrap bool) int {
	n := len(d.nodes)
	i := sort.Search(
		n,
		func(i int) bool {
			return d.nodes[i].Hash >= h
		},
	)

	if wrap && i == n {
		return 0
	}

	return i
}
