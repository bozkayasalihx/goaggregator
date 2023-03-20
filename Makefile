build: 
	go build -o bin/goaggr

buildl: 
	GOOS=linux go build -o bin/goaggr

run: build
	bin/goaggr


.PHONY: build buildl run
