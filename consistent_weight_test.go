package consistent

import (
	"fmt"
	"testing"
)

func TestConsistentWeight(t *testing.T) {
	c := New()
	c.Set(map[string]int{"Host1": 1, "Host3": 1, "Host2": 100})
	fmt.Println(c.Get("uri12")) // Host2

	c.UpdateWeight("Host1", 1000)
	fmt.Println(c.Get("uri12")) // Host2
}
