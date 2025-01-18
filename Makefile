export CGO_ENABLED=0

build:
	go build -o bin/iolyzer . 

run: build
	./bin/iolyzer
