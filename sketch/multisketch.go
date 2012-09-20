package sketch

type MultiSketch struct {
	Len      int
	Period   int
	Sketches []*Sketch
}

func MakeMultiSketch(l int, period int, k int, depth uint32, width uint32) *MultiSketch {
	sketches := make([]*Sketch, l)
	for i := range sketches {
		sketches[i] = MakeSketch(k, depth, width)
	}
	return &MultiSketch{l, period, sketches}
}

func (ms *MultiSketch) Update(term string) {
	for _, sk := range ms.Sketches {
		sk.Update(term)
	}
}

func (ms *MultiSketch) Top(n int) []Item {
	return ms.Sketches[0].Top(n)
}
