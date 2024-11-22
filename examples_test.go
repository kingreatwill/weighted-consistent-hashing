package consistent

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func TestAddAndList(t *testing.T) {
	members := []Member{}
	for i := 0; i < 8; i++ {
		member := testMember{fmt.Sprintf("node%d.olricmq", i), i + 1}
		members = append(members, member)
	}
	cfg := Config{
		PartitionCount:    71,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}

	c := New(members, cfg)
	owners := make(map[string]int)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owner := c.GetPartitionOwner(partID)
		owners[owner.String()]++
	}
	fmt.Println("average load:", c.AverageLoad())
	fmt.Println("owners:", owners)
}

func TestLoadDistribution(t *testing.T) {
	members := []Member{}
	for i := 0; i < 8; i++ {
		member := testMember{fmt.Sprintf("node%d.olricmq", i), i + 1}
		members = append(members, member)
	}
	cfg := Config{
		PartitionCount:    271,
		ReplicationFactor: 40,
		Load:              1.2,
		Hasher:            hasher{},
	}
	c := New(members, cfg)

	keyCount := 1000000
	load := (c.AverageLoad() * float64(keyCount)) / float64(cfg.PartitionCount)
	fmt.Println("Maximum key count for a member should be around this: ", math.Ceil(load))
	distribution := make(map[string]int)
	key := make([]byte, 4)
	for i := 0; i < keyCount; i++ {
		rand.Read(key)
		member := c.LocateKey(key)
		distribution[member.String()]++
	}
	for member, count := range distribution {
		fmt.Printf("member: %s, key count: %d\n", member, count)
	}
}

func TestRelocationPercentage(t *testing.T) {
	// Create a new consistent instance.
	members := []Member{}
	for i := 0; i < 8; i++ {
		member := testMember{fmt.Sprintf("node%d.olricmq", i), 1}
		members = append(members, member)
	}
	// Modify PartitionCount, ReplicationFactor and Load to increase or decrease
	// relocation ratio.
	cfg := Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := New(members, cfg)

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).String()
	}

	// Add a new member
	m := testMember{fmt.Sprintf("node%d.olricmq", 9), 1}
	c.Add(m)

	// Get the new layout and compare with the previous
	var changed int
	for partID, member := range owners {
		owner := c.GetPartitionOwner(partID)
		if member != owner.String() {
			changed++
			fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.String(), member)
		}
	}
	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/cfg.PartitionCount)
}

func TestSample(t *testing.T) {
	// Create a new consistent instance
	cfg := Config{
		PartitionCount:    2,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := New(nil, cfg)

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	node1 := testMember{"node1.olricmq.com", 1}
	c.Add(node1)

	node2 := testMember{"node100.olricmq.com", 1}
	c.Add(node2)

	node3 := testMember{"node30.olricmq.com", 1}
	c.Add(node3)

	mm, err := c.GetClosestN([]byte("my-key"), 3)
	fmt.Println(mm, err)

	node1Count, node2Count, node3Count := 0, 0, 0

	for i := 0; i <= 100000; i++ {
		key := []byte("my-key" + strconv.Itoa(i))
		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions is already distributed among members by Add function.
		owner := c.LocateKey(key)

		if owner.String() == "node1.olricmq.com" {
			node1Count++
		} else if owner.String() == "node100.olricmq.com" {
			node2Count++
		} else {
			node3Count++
		}
	}
	fmt.Println(node1Count, node2Count, node3Count)

}
