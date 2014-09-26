package sketch

type GroupSketch struct {
	SketchWithChildren
	Default string
	Parent  Interface
}

func MakeGroupSketch(def string, sketches map[string]Interface) *GroupSketch {
	return &GroupSketch{SketchWithChildren{sketches}, def, nil}
}

func (gs *GroupSketch) Top(n int) []Item {
	return gs.Sketches[gs.Default].Top(n)
}

func (ms *GroupSketch) Update(term string) {
	if ms.Parent != nil {
		ms.Parent.Update(term)
	}
	for _, sk := range ms.Sketches {
		sk.Update(term)
	}
}
