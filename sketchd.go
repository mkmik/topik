package main

import (
	"fmt"
	"net/http"
	"math/rand"
	"math"
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
	return uint32(rand.Int31()) << 1| 1;
}

func MakeTable(dx, dy uint32) [][]uint32 {
    table := make([][]uint32, dx) 
    for i := range table {
        table[i] = make([]uint32, dy) 
    }
    return table
}

func MakeHashes(depth uint32) []uint32 {
	var hashes = make([]uint32, depth);
	for i, _ := range hashes {
		hashes[i] = RandomOddInt();
	}
	return hashes;
}

type Sketch struct {
	K uint32
	LgWidth uint32
	Count [][]uint32
	HashFunctions []uint32
	Depth uint32
	// Heap
	// Map
}

func MakeSketch(k uint32, depth uint32, width uint32) Sketch {
	var m = uint(math.Ceil(math.Log2(float64(width))));
	var roundedWidth = uint32(1 << m);
	
	return Sketch {k, roundedWidth, MakeTable(depth, roundedWidth), MakeHashes(depth), depth};
}

func (self *Sketch) Update(key uint32) {
	var ix = key;
	var est uint32 = math.MaxUint32;

	for i := 0; uint32(i) < self.Depth; i++ {
		var hf = self.HashFunctions[i];
		var j = MultiplyShift(self.LgWidth, hf, ix)
		var x = self.Count[i][j];
		if x < est {
			est = x;
		}
		self.Count[i][j] = x + 1;
	}

	// self.UpdateHeap(key)
}

func (self *Sketch) Estimate(key uint32) uint32 {
	var ix = key;
	var est uint32 = math.MaxUint32;

	for i := 0; uint32(i) < self.Depth; i++ {
		var hf = self.HashFunctions[i];
		var j = MultiplyShift(self.LgWidth, hf, ix)
		var x = self.Count[i][j];
		if x < est {
			est = x;
		}
	}
	return est;
}

func main() {
	//var h Hello
	//http.ListenAndServe("localhost:4000",h)
	var table = MakeTable(10, 20);

	fmt.Printf("ciao %d\n", RandomOddInt())
	table[9][15] = 1;
	fmt.Printf("tab %d\n", table[9][15]);

	var sk = MakeSketch(200, 20, 500);
	fmt.Printf("tab %v\n", sk.HashFunctions);

	sk.Update(10);
	sk.Update(10);
	sk.Update(10);

	fmt.Printf("Found %v\n", sk.Estimate(10));
}
