package lru_cache

type Node struct {
	Key int
	Val []byte

	Prev *Node
	Next *Node
}

type LRU struct {
	capacity int
	cache    map[int]*Node

	left  *Node
	right *Node
}

func NewLRU(capacity int) *LRU {
	left, right := &Node{}, &Node{}

	left.Next = right
	right.Prev = left

	return &LRU{
		left:     left,
		right:    right,
		capacity: capacity,
		cache:    make(map[int]*Node),
	}
}

func (l *LRU) Put(key int, value []byte) {
	node, exists := l.cache[key]
	if exists {
		l.deleteNode(node)
	}

	node = &Node{Key: key, Val: value}
	l.cache[key] = node
	l.insertNode(node)

	if l.CapacityReached() {
		l.Evict()
	}
}

func (l *LRU) Get(key int) ([]byte, bool) {
	node, exists := l.cache[key]
	if !exists {
		return []byte{}, exists
	}

	l.deleteNode(node)
	l.insertNode(node)

	return node.Val, exists
}

func (l *LRU) CapacityReached() bool {
	return len(l.cache) > l.capacity
}

func (l *LRU) Evict() {
	lru := l.left.Next
	l.deleteNode(lru)

	delete(l.cache, lru.Key)
}

func (l *LRU) insertNode(node *Node) {
	prev, next := l.right.Prev, l.right

	node.Prev = prev
	node.Next = next

	prev.Next = node
	next.Prev = node
}

func (l *LRU) deleteNode(node *Node) {
	prev, next := node.Prev, node.Next

	prev.Next = next
	next.Prev = prev
}
