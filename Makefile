VERSION=$(shell git describe --tags --abbrev=0)

test:
	go test -v -p 1 -race -timeout 5m ./... -check.v

rebuild:
	go clean -i
	go build

all:
	go install github.com/mailgun/kafka-pixy
	go build -v -ldflags "-X main.Version=$(VERSION)" -o $(GOPATH)/bin/kafka-pixy-cli \
		github.com/mailgun/kafka-pixy/cmd/kafka-pixy-cli
	go install github.com/mailgun/kafka-pixy/tools/testproducer
	go install github.com/mailgun/kafka-pixy/tools/testconsumer

vet:
	go vet `go list ./... | grep -v '/vendor/'`

grpc:
	protoc -I . kafkapixy.proto --go_out=plugins=grpc:gen/golang --python_out=grpc:gen/python

errcheck: install_errcheck
	errcheck github.com/mailgun/kafka-pixy

fmt:
	$(eval $@_GOFILES_NO_VENDOR := $(shell find . -type f -name '*.go' -not -path "./vendor/*"))
	@if [ -n "$$(gofmt -l $($@_GOFILES_NO_VENDOR))" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

install_errcheck:
	go get github.com/kisielk/errcheck
