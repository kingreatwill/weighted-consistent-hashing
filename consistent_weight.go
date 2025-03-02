package consistent

import (
	"errors"
	"hash/crc32"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type uints []uint32

// Len returns the length of the uints array.
func (x uints) Len() int { return len(x) }

// Less returns true if element i is less than element j.
func (x uints) Less(i, j int) bool { return x[i] < x[j] }

// Swap exchanges elements i and j.
func (x uints) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// ErrEmptyCircle is the error returned when trying to get an element when nothing has been added to hash.
var ErrEmptyCircle = errors.New("empty circle")

type Member struct {
	Name   string
	Weight float64
}

// Consistent holds the information about the members of the consistent hash circle.
type Consistent struct {
	circle           map[uint32]string
	members          map[string]float64
	sortedHashes     uints
	NumberOfReplicas int
	count            int64
	scratch          [64]byte
	UseFnv           bool
	sync.RWMutex
}

// New creates a new Consistent object with a default setting of 20 replicas for each entry.
//
// To change the number of replicas, set NumberOfReplicas before adding entries.
func New(numberOfReplicas int) *Consistent {
	if numberOfReplicas <= 0 {
		numberOfReplicas = 20
	}
	c := new(Consistent)
	c.NumberOfReplicas = numberOfReplicas
	c.circle = make(map[uint32]string)
	c.members = make(map[string]float64)
	return c
}

// eltKey generates a string key for an element with an index.
func (c *Consistent) eltKey(elt string, idx int) string {
	// return elt + "|" + strconv.Itoa(idx)
	return strconv.Itoa(idx) + elt
}

// Add inserts a string element in the consistent hash.
func (c *Consistent) Add(elt string, wgt float64) {
	c.Lock()
	defer c.Unlock()
	c.add(elt, wgt)
}

// need c.Lock() before calling
func (c *Consistent) add(elt string, wgt float64) {
	if _, ok := c.members[elt]; ok {
		return
	}
	for i := 0; i < int(float64(c.NumberOfReplicas)*wgt); i++ {
		c.circle[c.hashKey(c.eltKey(elt, i))] = elt
	}
	c.members[elt] = wgt
	c.updateSortedHashes()
	c.count++
}

// Remove removes an element from the hash.
func (c *Consistent) Remove(elt string) {
	c.Lock()
	defer c.Unlock()
	c.remove(elt)
}

// need c.Lock() before calling
func (c *Consistent) remove(elt string) {
	wgt, ok := c.members[elt]
	if !ok {
		return
	}
	for i := 0; i < int(float64(c.NumberOfReplicas)*wgt); i++ {
		delete(c.circle, c.hashKey(c.eltKey(elt, i)))
	}
	delete(c.members, elt)
	c.updateSortedHashes()
	c.count--
}

// UpdateWeight update weight.
func (c *Consistent) UpdateWeight(elt string, wgt float64) {
	c.Lock()
	defer c.Unlock()
	c.updateWeight(elt, wgt)
}

// need c.Lock() before calling
func (c *Consistent) updateWeight(elt string, newWgt float64) {
	oldWgt, ok := c.members[elt]
	if !ok {
		return
	}
	if newWgt == oldWgt {
		return
	}
	if newWgt > oldWgt {
		for i := int(float64(c.NumberOfReplicas) * oldWgt); i < int(float64(c.NumberOfReplicas)*newWgt); i++ {
			c.circle[c.hashKey(c.eltKey(elt, i))] = elt
		}
	} else {
		for i := int(float64(c.NumberOfReplicas) * newWgt); i < int(float64(c.NumberOfReplicas)*oldWgt); i++ {
			delete(c.circle, c.hashKey(c.eltKey(elt, i)))
		}
	}
	c.members[elt] = newWgt
	c.updateSortedHashes()
}

// Set sets all the elements in the hash.  If there are existing elements not
// present in elts, they will be removed.
func (c *Consistent) Set(eltMap map[string]float64) {
	c.Lock()
	defer c.Unlock()
	for elt, wgt := range c.members {
		found := false
		for newElt, newWgt := range eltMap {
			if elt == newElt {
				if wgt != newWgt {
					c.updateWeight(elt, newWgt)
				}
				found = true
				break
			}
		}
		if !found {
			c.remove(elt)
		}
	}
	for newElt, newWgt := range eltMap {
		oldWgt, exists := c.members[newElt]
		if exists {
			if oldWgt != newWgt {
				c.updateWeight(newElt, newWgt)
			}
			continue
		}
		c.add(newElt, newWgt)
	}
}

func (c *Consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	var m []string
	for k := range c.members {
		m = append(m, k)
	}
	return m
}

// Get returns an element close to where name hashes to in the circle.
func (c *Consistent) Get(name string) (string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	return c.circle[c.sortedHashes[i]], nil
}

func (c *Consistent) search(key uint32) (i int) {
	f := func(x int) bool {
		return c.sortedHashes[x] > key
	}
	i = sort.Search(len(c.sortedHashes), f)
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return
}

// GetTwo returns the two closest distinct elements to the name input in the circle.
func (c *Consistent) GetTwo(name string) (string, string, error) {
	c.RLock()
	defer c.RUnlock()
	if len(c.circle) == 0 {
		return "", "", ErrEmptyCircle
	}
	key := c.hashKey(name)
	i := c.search(key)
	a := c.circle[c.sortedHashes[i]]

	if c.count == 1 {
		return a, "", nil
	}

	start := i
	var b string
	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		b = c.circle[c.sortedHashes[i]]
		if b != a {
			break
		}
	}
	return a, b, nil
}

// GetN returns the N closest distinct elements to the name input in the circle.
// weight = 0 can get
func (c *Consistent) GetN(name string, n int) ([]string, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.circle) == 0 {
		return nil, nil
	}

	if c.count < int64(n) {
		n = int(c.count)
	}

	var (
		key   = c.hashKey(name)
		i     = c.search(key)
		start = i
		res   = make([]string, 0, n)
		elem  = c.circle[c.sortedHashes[i]]
	)

	res = append(res, elem)

	if len(res) == n {
		return res, nil
	}

	for i = start + 1; i != start; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		elem = c.circle[c.sortedHashes[i]]
		if !sliceContainsMember(res, elem) {
			res = append(res, elem)
		}
		if len(res) == n {
			break
		}
	}

	return res, nil
}

// GetAll returns the N closest distinct elements to the name input in the circle.
func (c *Consistent) GetAll(name string) ([]string, error) {
	return c.GetN(name, int(c.count))
}

func (c *Consistent) hashKey(key string) uint32 {
	if c.UseFnv {
		return c.hashKeyFnv(key)
	}
	return c.hashKeyCRC32(key)
}

func (c *Consistent) hashKeyCRC32(key string) uint32 {
	if len(key) < 64 {
		var scratch [64]byte
		copy(scratch[:], key)
		return crc32.ChecksumIEEE(scratch[:len(key)])
	}
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) hashKeyFnv(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (c *Consistent) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	//reallocate if we're holding on to too much (1/4th)
	if cap(c.sortedHashes)/(c.NumberOfReplicas*4) > len(c.circle) {
		hashes = nil
	}
	for k := range c.circle {
		hashes = append(hashes, k)
	}
	sort.Sort(hashes)
	c.sortedHashes = hashes
}

func sliceContainsMember(set []string, member string) bool {
	for _, m := range set {
		if m == member {
			return true
		}
	}
	return false
}
