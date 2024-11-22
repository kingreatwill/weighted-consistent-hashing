package v2

import (
	"fmt"
	"strconv"
	"testing"
)

func TestAddAndList(t *testing.T) {
	ring := &Ring{
		WeightMultiplier: 1,
	}
	ring.Add("<member-1>", []byte("<key>"), 1)
	ring.Add("<member-2>", []byte("<key>"), 2)

	c1, c2 := 0, 0

	for i := 0; i < 100; i++ {
		m, ok := ring.Get([]byte("<key>" + strconv.Itoa(i)))
		if ok {
			if m == "<member-1>" {
				c1++
			} else {
				c2++
			}
		}
	}

	fmt.Println(c1, c2)
}
