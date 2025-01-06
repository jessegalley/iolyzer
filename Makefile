export CGO_ENABLED=0

build:
	go build -o bin/iolyzer cmd/iolyzer/main.go

run: build
	./bin/iolyzer
