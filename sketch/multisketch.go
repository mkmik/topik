package sketch

import (
	"fmt"
)

type MultiSketch struct {
	Len      int
	Period   int
	Sketches []*Sketch
	K        int
	Depth    uint32
	Width    uint32
}

func MakeMultiSketch(l int, period int, k int, depth uint32, width uint32) *MultiSketch {
	sketches := make([]*Sketch, l)
	for i := range sketches {
		sketches[i] = MakeSketch(k, depth, width)
	}
	return &MultiSketch{l, period, sketches, k, depth, width}
}

func (ms *MultiSketch) Update(term string) {
	for _, sk := range ms.Sketches {
		sk.Update(term)
	}
}

func (ms *MultiSketch) Top(n int) []Item {
	return ms.Sketches[0].Top(n)
}

func (ms *MultiSketch) Rotate() {
	ms.Sketches = ms.Sketches[1:]
	ms.Sketches = append(ms.Sketches, MakeSketch(ms.K, ms.Depth, ms.Width))
}
