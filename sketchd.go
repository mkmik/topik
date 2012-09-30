package main

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"
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

type Configuration struct {
	File     string
	Preload  bool
	Autosave time.Duration
	Sketches map[string]SketchDef
}

type SketchDef struct {
	Type     string
	Length   int
	Period   int
	K        int
	Depth    uint32
	Width    uint32
	Default  string
	Parent   string
	Sketches map[string]SketchDef
}

func StartAutoRotation(sketches map[string]sketch.Interface) {
	for _, sk := range sketches {
		sk.StartAutoRotation()
	}
}

func StopAutoRotation(sketches map[string]sketch.Interface) {
	for _, sk := range sketches {
		sk.StopAutoRotation()
	}
}

func ParseSketches(defs map[string]SketchDef) (sketches map[string]sketch.Interface) {
	sketches = make(map[string]sketch.Interface)

	for k, c := range defs {
		var sk sketch.Interface

		switch c.Type {
		case "Multi":
			sk = sketch.MakeMultiSketch(c.Length, c.Period, c.K, c.Depth, c.Width)
		case "Group":
			sk = sketch.MakeGroupSketch(c.Default, c.Parent, ParseSketches(c.Sketches))
		default:
			sk = sketch.MakeSketch(c.K, c.Depth, c.Width)
		}
		sketches[k] = sk
	}

	return
}

func main() {
	file, e := ioutil.ReadFile("./config.json")
	if e != nil {
		fmt.Printf("File error: %v\n", e)
		os.Exit(1)
	}

	var conf Configuration
	json.Unmarshal(file, &conf)

	sketches := ParseSketches(conf.Sketches)

	gob.Register(sketch.MakeSketch(1, 1, 1))
	gob.Register(sketch.MakeMultiSketch(1, 0, 1, 1, 1))
	gob.Register(sketch.MakeGroupSketch("", "", nil))
	gob.Register(&sketch.Item{})

	if conf.Preload {
		fmt.Printf("Preloading\n")
		for _, sk := range sketches {
			Preload(sk)
		}
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
		var cname = name
		http.HandleFunc("/top/"+name, func(w http.ResponseWriter, r *http.Request) {
			js, _ := json.Marshal(sketches[cname].Top(5))
			w.Write(js)
		})

		switch gs := sk.(type) {
		case *sketch.MultiSketch:
			var cname = name
			http.HandleFunc("/top/"+name+"/rotate", func(w http.ResponseWriter, r *http.Request) {
				sketches[cname].(*sketch.MultiSketch).Rotate()
			})
		case *sketch.GroupSketch:
			var cname = name
			for child := range gs.Sketches {
				fmt.Printf("child of group %v: %v\n", cname, child)
				http.HandleFunc("/top/"+name+"/"+child, func(w http.ResponseWriter, r *http.Request) {
					js, _ := json.Marshal(sketches[cname].(*sketch.GroupSketch).Sketches[child].Top(5))
					w.Write(js)
				})
			}
		}
	}

	http.HandleFunc("/add", func(w http.ResponseWriter, r *http.Request) {
		terms := r.URL.Query()["term"]
		for _, t := range terms {
			update <- t
		}
	})

	dump := func(w io.Writer) {
		dumpDir := filepath.Dir(conf.File)
		wfile, err := ioutil.TempFile(dumpDir, "topk-")

		if err != nil {
			fmt.Fprintf(w, "Cannot write: %v\n", err)
			return
		}
		defer wfile.Close()
		defer func() {
			os.Remove(wfile.Name())
		}()

		file, err := gzip.NewWriterLevel(wfile, gzip.BestCompression)
		if err != nil {
			fmt.Fprintf(w, "Cannot open compressed stream: %v\n", err)
			return
		}
		defer file.Close()

		enc := gob.NewEncoder(file)

		err = enc.Encode(sketches)
		if err != nil {
			fmt.Fprintf(w, "Cannot serialize: %v\n", err)
			return
		}

		os.Rename(wfile.Name(), conf.File)
	}

	load := func(w io.Writer) {
		StopAutoRotation(sketches)
		defer StartAutoRotation(sketches)

		rfile, err := os.Open(conf.File)

		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			fmt.Fprintf(w, "Cannot open: %v\n", err)
			return
		}

		defer rfile.Close()

		file, err := gzip.NewReader(rfile)
		if err != nil {
			fmt.Fprintf(w, "Cannot open compressed stream: %v\n", err)
			return
		}
		defer rfile.Close()

		enc := gob.NewDecoder(file)

		err = enc.Decode(&sketches)
		if err != nil {
			fmt.Fprintf(w, "Cannot deserialize: %v\n", err)
			return
		}

	}

	http.HandleFunc("/dump", func(w http.ResponseWriter, r *http.Request) {
		dump(w)
		fmt.Fprintf(w, "ok\n")
	})

	if !conf.Preload {
		load(os.Stderr)
	}

	StartAutoRotation(sketches)

	if conf.Autosave > 0 {
		go func() {
			for {
				time.Sleep(conf.Autosave * time.Second)
				dump(os.Stderr)
			}
		}()
	}

	http.ListenAndServe("localhost:4000", nil)
}
