package sketch

import (
	"log/syslog"
	"time"
)

var log, _ = syslog.NewLogger(syslog.LOG_INFO, 0)

type MultiSketch struct {
	Len      int
	Period   int
	Sketches []*Sketch
	K        int
	Depth    uint32
	Width    uint32
	rotor    chan chan int
}

func MakeMultiSketch(l int, period int, k int, depth uint32, width uint32) *MultiSketch {
	sketches := make([]*Sketch, l)
	for i := range sketches {
		sketches[i] = MakeSketch(k, depth, width)
	}
	return &MultiSketch{l, period, sketches, k, depth, width, nil}
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

func (ms *MultiSketch) StartAutoRotation() {
	if ms.Period == 0 {
		ms.rotor = nil
		return
	}

	go func() {
		if ms.rotor == nil {
			ms.rotor = make(chan chan int)
		}

		select {
		case <-time.After(time.Duration(ms.Period/ms.Len) * time.Second):
			log.Printf("Rotating topk after %d seconds", ms.Period/ms.Len)
			ms.Rotate()
		case ans := <-ms.rotor:
			log.Printf("Aborting topk rotation")
			ans <- 0
			return
		}
	}()
}

func (ms *MultiSketch) StopAutoRotation() {
	if ms.rotor != nil {
		res := make(chan int)
		ms.rotor <- res
		<-res
	}
}
