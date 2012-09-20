package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"topk/sketch"
)

type Hello struct{}

func DumpTop(sk sketch.Interface, n, l int, o bool) {
	items := sk.Top(n)

	fmt.Fprintf(os.Stderr, "-----------\n")
	if n > 0 {
		fmt.Fprintf(os.Stderr, "TOP %d (%d lines)\n", n, l)
	} else {
		fmt.Fprintf(os.Stderr, "TOP %d\n")
	}

	f := os.Stderr
	if o {
		f = os.Stdout
	}

	for _, v := range items {
		fmt.Fprintf(f, "%d %s\n", v.Est, v.Key)
	}
}

func Preload(sk sketch.Interface) {
	//file, err := os.Open("body.txt")
	file, err := os.Open("short.txt")
	if err != nil {
		fmt.Printf("cannot open\n")
	}

	bf := bufio.NewReader(file)

	n := 0

	for {
		line, _, err := bf.ReadLine()
		if err != nil {
			break
		}
		sk.Update(string(line))

		if n%100000 == 0 {
			DumpTop(sk, 5, n, false)
		}
		n = n + 1
	}

	DumpTop(sk, 5, 0, true)
}

type jsonobject map[string]SketchDef

type SketchDef struct {
	Type   string
	Length int
	Period int
	K      int
	Depth  uint32
	Width  uint32
}

func main() {
	sketches := make(map[string]sketch.Interface)

	file, e := ioutil.ReadFile("./config.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	var conf jsonobject
	json.Unmarshal(file, &conf)

	for k, c := range conf {
		var sk sketch.Interface

		if c.Type == "Multi" {
			sk = sketch.MakeMultiSketch(c.Length, c.Period, c.K, c.Depth, c.Width)
		} else {
			sk = sketch.MakeSketch(c.K, c.Depth, c.Width)
		}
		sketches[k] = sk
	}

	//for _, sk := range sketches {
	//	Preload(sk)
	//}

	update := make(chan string, 2000)
	go func() {
		for t := range update {
			for _, sk := range sketches {
				sk.Update(t)
			}
		}
	}()

	for name, sk := range sketches {
		var csk = sk
		http.HandleFunc("/top/"+name, func(w http.ResponseWriter, r *http.Request) {
			js, _ := json.Marshal(csk.Top(5))
			w.Write(js)
		})

		switch ms := sk.(type) {
		case *sketch.MultiSketch:
			var cms = ms
			http.HandleFunc("/top/"+name+"/rotate", func(w http.ResponseWriter, r *http.Request) {
				cms.Rotate()
			})
		}
	}

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		terms := r.URL.Query()["term"]
		for _, t := range terms {
			update <- t
		}
	})

	http.ListenAndServe("localhost:4000", nil)
}
