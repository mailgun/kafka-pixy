# convenience command to update and re-vendor all dependencies
godep:
	godep update ...
	godep save -r ./...

test:
	go test -v -p 1 -race -timeout 5m ./... -check.v

rebuild:
	go clean -i
	go build

all:
	go install github.com/mailgun/kafka-pixy
	go install github.com/mailgun/kafka-pixy/tools/testproducer
	go install github.com/mailgun/kafka-pixy/tools/testconsumer

vet:
	go vet `go list ./... | grep -v '/vendor/'`

grpc:
	protoc -I . kafkapixy.proto --go_out=plugins=grpc:gen/golang

errcheck: install_errcheck
	errcheck github.com/mailgun/kafka-pixy

fmt:
	@if [ -n "$$(go fmt ./...)" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

install_errcheck:
	go get github.com/kisielk/errcheck
