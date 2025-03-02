package consistent

import (
	"math/rand/v2"
	"sort"
)

type WeightedConsistent struct {
	name       string
	c          *Consistent
	rawMembers []Member
	cMembers   map[string]float64
}

func NewWeightedConsistent(name string, numberOfReplicas int, members []Member) *WeightedConsistent {
	minW := 0.0
	for _, m := range members {
		if m.Weight > 0 {
			if minW == 0 {
				minW = m.Weight
			}
			if minW > m.Weight {
				minW = m.Weight
			}
		}
	}
	// 权重按照比例缩放, 非0最小的权重为1
	eltMap := make(map[string]float64)
	for _, m := range members {
		if m.Weight > 0 {
			w := m.Weight / minW
			eltMap[m.Name] = w
		}
	}
	cons := &WeightedConsistent{
		name:       name,
		c:          nil,
		rawMembers: members,
		cMembers:   eltMap,
	}
	if numberOfReplicas <= 0 {
		numberOfReplicas = 200
	}
	c := New(numberOfReplicas)
	if len(eltMap) > 0 {
		c.Set(eltMap)
	}
	cons.c = c
	return cons
}

// GetAll 一致性hash加权随机
func (c *WeightedConsistent) GetAll(key string) ([]string, error) {
	return c.c.GetAll(key)
}

// GetRandomAll 加权随机
func (c *WeightedConsistent) GetRandomAll(key string) ([]string, error) {
	return WeightedShuffle(c.cMembers), nil
}

func (c *WeightedConsistent) Len() int {
	return len(c.cMembers)
}

func WeightedShuffle(cMembers map[string]float64) []string {
	// 为每个项目生成随机权重
	weightedRandom := make([]struct {
		name   string
		random float64
	}, 0, len(cMembers))
	for name, weight := range cMembers {
		weightedRandom = append(weightedRandom, struct {
			name   string
			random float64
		}{
			name:   name,
			random: rand.Float64() * weight,
		})
	}
	// 按随机权重排序
	sort.Slice(weightedRandom, func(i, j int) bool {
		return weightedRandom[i].random > weightedRandom[j].random
	})
	// 提取排序后的项目
	result := make([]string, len(weightedRandom))
	for i, wr := range weightedRandom {
		result[i] = wr.name
	}
	return result
}
