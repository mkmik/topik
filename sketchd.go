package main

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"topk/pqueue"
)

type Hello struct{}

func (h Hello) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request) {
	fmt.Fprint(w, "Hello!")
}

const IntSize = 32
const IntMask = (1 << IntSize) - 1

func MultiplyShift(m uint32, a uint32, x uint32) uint32 {
	return ((a * x) & IntMask) >> (IntSize - m)
}

func RandomOddInt() uint32 {
	return uint32(rand.Int31())<<1 | 1
}

func MakeTable(dx, dy uint32) [][]uint32 {
	table := make([][]uint32, dx)
	for i := range table {
		table[i] = make([]uint32, dy)
	}
	return table
}

func MakeHashes(depth uint32) []uint32 {
	var hashes = make([]uint32, depth)
	for i, _ := range hashes {
		hashes[i] = RandomOddInt()
	}
	return hashes
}

type Sketch struct {
	K             int
	LgWidth       uint32
	Count         [][]uint32
	HashFunctions []uint32
	Depth         uint32
	Heap          *pqueue.Queue
	Map           map[uint32]Item
}

func MakeSketch(k int, depth uint32, width uint32) Sketch {
	var m = uint(math.Ceil(math.Log2(float64(width))))
	var roundedWidth = uint32(1 << m)

	return Sketch{k, uint32(m), MakeTable(depth, roundedWidth),
		MakeHashes(depth), depth, pqueue.New(0), make(map[uint32]Item)}
}

func (self *Sketch) Update(key uint32) {
	var ix = key
	var est uint32 = math.MaxUint32

	for i := 0; uint32(i) < self.Depth; i++ {
		var hf = self.HashFunctions[i]
		var j = MultiplyShift(self.LgWidth, hf, ix)
		var x = self.Count[i][j]
		if x < est {
			est = x
		}
		self.Count[i][j] = x + 1
	}

	self.UpdateHeap(key, est)
}

func (self *Sketch) Estimate(key uint32) uint32 {
	var ix = key
	var est uint32 = math.MaxUint32

	for i := 0; uint32(i) < self.Depth; i++ {
		var hf = self.HashFunctions[i]
		var j = MultiplyShift(self.LgWidth, hf, ix)
		var x = self.Count[i][j]
		if x < est {
			est = x
		}
	}
	return est
}

type Item struct {
	est uint32
	val uint32
}

func (t *Item) Less(other interface{}) bool {
	return t.est < other.(*Item).est
}

func (self *Sketch) UpdateHeap(key uint32, est uint32) {
	//	fmt.Printf("Updating heap %v %v\n", key, est)
	if self.Heap.Len() == 0 || self.Heap.Peek().(*Item).est < est {
		//		fmt.Printf("empty heap or adding bigger than min\n")
		probe, ok := self.Map[key]
		if !ok {
			//			fmt.Printf("not found in map\n")
			if len(self.Map) < self.K {
				//				fmt.Printf("Still growing\n")
				entry := Item{est, key}
				self.Heap.Enqueue(&entry)
				self.Map[key] = entry
			} else {
				fmt.Printf("TODO Push this guy out\n")
			}
		} else {
			//			fmt.Printf("found in map\n")
			probe.est = est
			self.Heap.Heapify()
		}
	}
}

func main() {
	//var h Hello
	//http.ListenAndServe("localhost:4000",h)
	var table = MakeTable(10, 20)

	table[9][15] = 1
	//	fmt.Printf("tab %d\n", table[9][15])

	var sk = MakeSketch(200, 20, 500)
	//	fmt.Printf("tab %v\n", sk.HashFunctions)

	sk.Update(10)
	sk.Update(10)
	sk.Update(10)

	sk.Update(12)
	sk.Update(12)

	fmt.Printf("Found 10 -> %v\n", sk.Estimate(10))
	fmt.Printf("Found 12 -> %v\n", sk.Estimate(12))

	fmt.Printf("----------------- tests\n")

	for k, _ := range sk.Map {
		fmt.Printf("Map %v %v\n", k, sk.Estimate(k))
	}

	/*
		q := pqueue.New(0)
		q.Enqueue(&Item{23, 2})
		q.Enqueue(&Item{2, 10})
		q.Enqueue(&Item{3, 5})
		q.Enqueue(&Item{10, 7})

		fmt.Printf("Head %v\n", q.Len())
		fmt.Printf("Head %v\n", q.Peek().(*Item).est)

		for i := 0; i < 4; i += 1 {
			item := q.Dequeue()
			fmt.Printf("Dequeued %v\n", item)
		}
	*/
}
