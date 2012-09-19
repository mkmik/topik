package main

import (
	"bufio"
	"fmt"
	"net/http"
	"encoding/json"
	"os"
	"topk/sketch"
)

type Hello struct{}

func DumpTop(sk sketch.Sketch, n, l int, o bool) {
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

func Preload(sk sketch.Sketch) {
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
	var sk = sketch.MakeSketch(200, 10, 1000)

//	Preload(sk)

	http.HandleFunc("/top", func(w http.ResponseWriter, r *http.Request) {
		js, _ := json.Marshal(sk.Top(5))
		w.Write(js)
	})

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		terms := r.URL.Query()["term"]
		for _, t := range terms {
			sk.Update(t)
		}
	})


	http.ListenAndServe("localhost:4000", nil)
}
