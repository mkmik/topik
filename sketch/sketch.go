package sketch

import (
	"hash"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"topk/pqueue"
)

const IntSize = 64
const IntMask = (1 << IntSize) - 1

func MultiplyShift(m uint, a uint64, x uint64) uint64 {
	return ((a * x) & IntMask) >> (IntSize - m)
}

func RandomOddInt() uint64 {
	return uint64(rand.Int63())<<1 | 1
}

func MakeTable(dx, dy uint32) [][]uint32 {
	table := make([][]uint32, dx)
	for i := range table {
		table[i] = make([]uint32, dy)
	}
	return table
}

func MakeHashes(depth uint32) []uint64 {

	var hashes = make([]uint64, depth)
	for i, _ := range hashes {
		hashes[i] = RandomOddInt()
	}

	return hashes
}

type Interface interface {
	Update(term string)
	Top(n int) []Item
}

type Sketch struct {
	K             int
	LgWidth       uint
	Count         [][]uint32
	HashFunctions []uint64
	Depth         uint32
	Heap          *pqueue.Queue
	Map           map[string]Item
	hasher        hash.Hash64
}

func MakeSketch(k int, depth uint32, width uint32) *Sketch {
	var m = uint(math.Ceil(math.Log2(float64(width))))
	var roundedWidth = uint32(1 << m)

	return &Sketch{k, m, MakeTable(depth, roundedWidth),
		MakeHashes(depth), depth, pqueue.New(0), make(map[string]Item), fnv.New64()}
}

func (self *Sketch) DHash(key string, hf uint64) uint64 {
	self.hasher.Reset()
	self.hasher.Write([]byte(key))
	self.hasher.Write(SerializeUint64(hf))
	return self.hasher.Sum64()
}

func (self *Sketch) Hash(key string) uint64 {
	self.hasher.Reset()
	self.hasher.Write([]byte(key))
	return self.hasher.Sum64()
}

func (self *Sketch) estimateUpdate(key string, update bool) uint32 {
	var ix = self.Hash(key)
	var est uint32 = math.MaxUint32

	for i := 0; uint32(i) < self.Depth; i++ {
		var hf = self.HashFunctions[i]
		var j = MultiplyShift(self.LgWidth, hf, ix)
		//var j = MultiplyShift(self.LgWidth, hf, self.Hash(key, hf))
		var x = self.Count[i][j]
		if x < est {
			est = x
		}
		if update {
			self.Count[i][j] = x + 1
		}
	}

	if update {
		self.UpdateHeap(key, est)
	}

	return est
}

func (self *Sketch) Update(key string) { self.estimateUpdate(key, true) }

func (self *Sketch) Estimate(key string) uint32 { return self.estimateUpdate(key, false) }

type Item struct {
	Est uint32
	Key string
}

func (t *Item) Less(other interface{}) bool {
	return t.Est < other.(*Item).Est
}

func (self *Sketch) UpdateHeap(key string, est uint32) {
	if self.Heap.Len() == 0 || self.Heap.Peek().(*Item).Est < est {
		probe, ok := self.Map[key]
		if !ok {
			if len(self.Map) < self.K {
				entry := Item{est, key}
				self.Heap.Enqueue(&entry)
				self.Map[key] = entry
			} else {
				entry := Item{est, key}
				self.Heap.Enqueue(&entry)
				old := self.Heap.Dequeue()
				delete(self.Map, old.(*Item).Key)
				self.Map[key] = entry
			}
		} else {
			probe.Est = est
			self.Heap.Heapify()
		}
	}
}

func SerializeUint64(n uint64) []byte {
	return []byte{byte((n >> 0) & 0xFF),
		byte((n >> 8) & 0xFF),
		byte((n >> 16) & 0xFF),
		byte((n >> 24) & 0xFF),
		byte((n >> 32) & 0xFF),
		byte((n >> 40) & 0xFF),
		byte((n >> 48) & 0xFF),
		byte((n >> 56) & 0xFF)}
}

type Items []Item

func (s Items) Len() int           { return len(s) }
func (s Items) Less(i, j int) bool { return s[i].Est > s[j].Est }
func (s Items) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (sk *Sketch) Top(n int) []Item {
	items := make(Items, 0)
	for k, _ := range sk.Map {
		entry := Item{sk.Estimate(k), k}
		items = append(items, entry)
	}

	sort.Sort(items)
	b := n
	if b >= len(items) {
		b = len(items)
	}

	return items[0:b]
}
