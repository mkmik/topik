package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"topk/sketch"
)

type Hello struct{}

func (h Hello) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request) {
	fmt.Fprint(w, "Hello!")
}

func main() {
	//var h Hello
	//http.ListenAndServe("localhost:4000",h)
	//	fmt.Printf("tab %d\n", table[9][15])

	var sk = sketch.MakeSketch(200, 10, 1000)
	//	fmt.Printf("tab %v\n", sk.HashFunctions)

	fmt.Fprintf(os.Stderr, "----------------- tests\n")

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
			sketch.DumpTop(sk, 5, n, false)
		}
		n = n + 1
	}

	sketch.DumpTop(sk, 5, 0, true)
}
