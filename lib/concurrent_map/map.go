package concurrent_map

import "sync"

type Map[K, V any] struct {
	cMap sync.Map
}

func NewMap[K, V any]() Map[K, V] {
	return Map[K, V]{}
}

func (m *Map[K, V]) Get(k K) (*V, bool) {
	v, exists := m.cMap.Load(k)
	if !exists {
		return nil, false
	}

	val := v.(V)
	return &val, true
}

func (m *Map[K, V]) Set(k K, v V) {

	m.cMap.Store(k, v)
}

func (m *Map[K, V]) Delete(k K) {
	m.cMap.Delete(k)
}

func (m *Map[K, V]) Range(f func(k any, v any) bool) {
	m.cMap.Range(f)
}
