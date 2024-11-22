package v2

// member is a member that is placed on the ring.
type member struct {
	ID     string
	Key    []byte
	Weight uint32
}

// node is a point on the hash ring.
type node struct {
	Hash    uint32
	Members []*member
}

// Add adds a member to this node.
func (n *node) Add(m *member) {
	for i, x := range n.Members {
		if isHigherPriority(m, x) {
			n.Members = append(n.Members, nil)   // grow by 1
			copy(n.Members[i+1:], n.Members[i:]) // move everything after i back
			n.Members[i] = m                     // replace i
			return
		}
	}

	n.Members = append(n.Members, m)
}

// Remove removes a member from this node.
func (n *node) Remove(m *member) {
	for i, x := range n.Members {
		if x.ID == m.ID {
			n.Members = append(n.Members[:i], n.Members[i+1:]...)
			return
		}
	}
}

// isHigherPriority returns true if a should be used in preference to b.
func isHigherPriority(a, b *member) bool {
	if a.Weight == b.Weight {
		return a.ID < b.ID
	}

	return a.Weight > b.Weight
}
