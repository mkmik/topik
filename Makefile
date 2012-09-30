
.PHONY: sketchd run

sketchd: 
	go build sketchd.go

run:
	go run sketchd.go
