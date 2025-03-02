package consistent

import (
	"fmt"
	"testing"
)

func TestConsistentWeight(t *testing.T) {
	c := New(20)
	c.Set(map[string]float64{"Host1": 1, "Host3": 1, "Host2": 100})
	fmt.Println(c.Get("uri12")) // Host2

	c.UpdateWeight("Host1", 1000)
	fmt.Println(c.Get("uri12")) // Host1
}

func TestWeightedConsistent(t *testing.T) {
	c := NewWeightedConsistent("123", 200, []Member{{"A10", 10}, {"B10", 10}, {"C100", 100}})
	for i := 0; i < 20; i++ {
		fmt.Println(c.GetAll(fmt.Sprintf("%d", i)))
	}
}
