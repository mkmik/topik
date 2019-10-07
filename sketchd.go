package main

import (
	"bufio"
	"compress/gzip"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
	"github.com/mkmik/topik/sketch"
)

var encodingError = errors.New("Encoding error")

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
	Format   string
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

func ParseSketches(defs map[string]SketchDef) (sketches *sketch.GroupSketch) {
	sketches = sketch.MakeGroupSketch("all", make(map[string]sketch.Interface))

	groupParents := map[*sketch.GroupSketch]string{}

	for k, c := range defs {
		var sk sketch.Interface

		switch c.Type {
		case "Multi":
			sk = sketch.MakeMultiSketch(c.Length, c.Period, c.K, c.Depth, c.Width)
		case "Group":
			sub := ParseSketches(c.Sketches)

			gs := sketch.MakeGroupSketch(c.Default, sub.Sketches)
			groupParents[gs] = c.Parent
			sk = gs
		default:
			sk = sketch.MakeSketch(c.K, c.Depth, c.Width)
		}
		sketches.Sketches[k] = sk
	}

	for g, p := range groupParents {
		parent, ok := sketches.Sketches[p]
		if !ok && p != "" {
			log.Fatalf("Invalid parent %q for sketch", p)
		}
		g.Parent = parent
	}

	return
}

type FileFormat interface {
	Encode(conf Configuration, sketches *sketch.GroupSketch, file io.Writer) error
	Decode(conf Configuration, sketches *sketch.GroupSketch, file io.Reader) error
}

type GobFormat struct {
}

func (g GobFormat) Encode(conf Configuration, sketches *sketch.GroupSketch, file io.Writer) error {
	enc := gob.NewEncoder(file)
	return enc.Encode(sketches.Sketches)
}

func (g GobFormat) Decode(conf Configuration, sketches *sketch.GroupSketch, file io.Reader) error {
	var sk map[string]sketch.Interface
	dec := gob.NewDecoder(file)
	err := dec.Decode(&sk)
	if err == nil {
		sketches.Sketches = sk
	}

	return err
}

type JsonFormat struct {
}

func SketchToJson(sk sketch.Interface) interface{} {
	switch gs := sk.(type) {
	case *sketch.Sketch:
		res := make(map[string]interface{})
		res["heap"] = gs.Heap
		res["count"] = gs.Count
		res["hashes"] = gs.HashFunctions
		return res
	case *sketch.MultiSketch:
		children := make([]interface{}, 0)
		for _, child := range gs.Sketches {
			children = append(children, SketchToJson(child))
		}
		return children
	case *sketch.GroupSketch:
		children := make(map[string]interface{})
		for name, child := range gs.Sketches {
			children[name] = SketchToJson(child)
		}
		return children
	}

	return nil
}

func (g JsonFormat) Encode(conf Configuration, sketches *sketch.GroupSketch, file io.Writer) error {
	fmt.Printf("JSON ENCODING\n")
	root := make(map[string]interface{})
	root["conf"] = conf
	root["data"] = SketchToJson(sketches)
	js, err := json.Marshal(root)
	file.Write(js)
	return err
}

func (g JsonFormat) Decode(conf Configuration, sketches *sketch.GroupSketch, file io.Reader) error {
	fmt.Printf("JSON READING\n")
	return nil
}

type topHandler struct {
	sketch.Interface
}

func (t topHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	js, _ := json.Marshal(t.Top(5))
	w.Write(js)
}

type rotateHandler struct {
	*sketch.MultiSketch
}

func (s rotateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Rotate()
}

type addHandler struct {
	sketch.Interface
}

func (s addHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	terms := r.URL.Query()["term"]
	for _, t := range terms {
		s.Update(t)
	}
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
	gob.Register(sketch.MakeGroupSketch("", nil))
	gob.Register(&sketch.Item{})

	/*
		if conf.Preload {
			fmt.Printf("Preloading\n")
			for _, sk := range sketches {
				Preload(sk)
			}
		}
	*/

	for name, sk := range sketches.Sketches {
		http.Handle("/top/"+name, topHandler{sk})
		http.Handle("/add/"+name, addHandler{sk})

		switch gs := sk.(type) {
		case *sketch.MultiSketch:
			http.Handle("/rotate/"+name, rotateHandler{gs})
		case *sketch.GroupSketch:
			for childName, child := range gs.Sketches {
				fmt.Printf("child of group %v: %v\n", name, childName)
				http.Handle("/top/"+name+"/"+childName, topHandler{child})

				if msChild, ok := child.(*sketch.MultiSketch); ok {
					http.Handle("/rotate/"+name+"/"+childName, rotateHandler{msChild})
				}
			}
		}
	}

	formats := make(map[string]FileFormat)
	formats["gob"] = &GobFormat{}
	formats["json"] = &JsonFormat{}

	save := func(fileName string, w io.Writer, format FileFormat) {
		fmt.Fprintf(w, "saving\n")

		dumpDir := filepath.Dir(fileName)
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

		err = format.Encode(conf, sketches, file)
		if err != nil {
			fmt.Fprintf(w, "Cannot serialize: %v\n", err)
			return
		}

		os.Rename(wfile.Name(), fileName)
	}

	dump := func(w io.Writer) {
		save(conf.File, w, formats[conf.Format])
	}

	parse := func(w io.Writer, format FileFormat) {
		StopAutoRotation(sketches.Sketches)
		defer StartAutoRotation(sketches.Sketches)

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

		err = format.Decode(conf, sketches, file)

		if err != nil {
			fmt.Fprintf(w, "Cannot deserialize: %v\n", err)
			return
		}
	}

	load := func(w io.Writer) {
		parse(w, formats[conf.Format])
	}

	http.HandleFunc("/dump", func(w http.ResponseWriter, r *http.Request) {
		dump(w)
		fmt.Fprintf(w, "ok\n")
	})

	http.HandleFunc("/dump.json", func(w http.ResponseWriter, r *http.Request) {
		save(conf.File+".json", w, formats["json"])
		fmt.Fprintf(w, "ok\n")
	})

	if !conf.Preload {
		if true {
			load(os.Stderr)
		}
	}

	StartAutoRotation(sketches.Sketches)

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
