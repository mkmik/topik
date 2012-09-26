package sketch

import ()

type GroupSketch struct {
	SketchWithChildren
	Default string
	Parent  string
}

func MakeGroupSketch(def string, parent string, sketches map[string]Interface) *GroupSketch {
	return &GroupSketch{SketchWithChildren{sketches}, def, parent}
}

func (gs *GroupSketch) Top(n int) []Item {
	return gs.Sketches[gs.Default].Top(n)
}

func (ms *GroupSketch) Update(term string) {
	for _, sk := range ms.Sketches {
		sk.Update(term)
	}
}
