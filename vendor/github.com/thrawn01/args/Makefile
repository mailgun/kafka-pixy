.PHONY: test examples all get-deps
.DEFAULT_GOAL := all

# GO
GOPATH := $(shell go env | grep GOPATH | sed 's/GOPATH="\(.*\)"/\1/')
GLIDE := $(GOPATH)/bin/glide
PATH := $(GOPATH)/bin:$(PATH)
export $(PATH)

bin/chicken-cli: examples/chicken-cli/checkin-cli.go
	go build -o bin/chicken-cli examples/chicken-cli/checkin-cli.go

bin/demo: examples/demo/demo.go
	go build -o bin/demo examples/demo/demo.go

bin/watch: examples/watch/watch.go
	go build -o bin/watch examples/watch/watch.go

examples: bin/chicken-cli bin/demo bin/watch

all: test examples

travis-ci: get-deps
	go get -u github.com/mattn/goveralls
	go get -u golang.org/x/tools/cmd/cover
	go get -u golang.org/x/text/encoding
	goveralls -service=travis-ci

$(GLIDE):
	go get -u github.com/Masterminds/glide

get-deps: $(GLIDE)
	$(GLIDE) install
	go get -u golang.org/x/net/context
	go get -u golang.org/x/text/encoding

clean:
	rm bin/*

test:
	go test .
