package main

import (
	"bufio"
	"encoding/json"
	"fmt"
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

func main() {
	sketches := make(map[string]sketch.Interface)
	sketches["hourly"] = sketch.MakeMultiSketch(10, 3600/10, 200, 10, 1000)
	sketches["weekly"] = sketch.MakeMultiSketch(7, 86400, 200, 10, 1000)
	sketches["monthly"] = sketch.MakeMultiSketch(10, 86400*24*30/10, 10, 10, 1000)
	sketches["all"] = sketch.MakeSketch(200, 10, 1000)

	for _, sk := range sketches {
		Preload(sk)
	}

	update := make(chan string, 2000)
	go func() {
		for t := range update {
			for _, sk := range sketches {
				sk.Update(t)
			}
		}
	}()

	for name, sk := range sketches {
		http.HandleFunc("/top/"+name, func(w http.ResponseWriter, r *http.Request) {
			js, _ := json.Marshal(sk.Top(5))
			w.Write(js)
		})
	}

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		terms := r.URL.Query()["term"]
		for _, t := range terms {
			update <- t
		}
	})

	http.ListenAndServe("localhost:4000", nil)
}
