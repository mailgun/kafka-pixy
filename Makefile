# convenience command to update and re-vendor all dependencies
godep:
	godep update ...
	godep save -r ./...

test:
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/admin -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/config -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/consumer -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/logging -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/prettyfmt -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/producer -check.v
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/service -check.v

rebuild:
	go clean -i
	go build

all:
	go install github.com/mailgun/kafka-pixy
	go install github.com/mailgun/kafka-pixy/tools/testproducer
	go install github.com/mailgun/kafka-pixy/tools/testconsumer

vet: install_go_vet
	go vet ./...

errcheck: install_errcheck
	errcheck github.com/mailgun/kafka-pixy

fmt:
	@if [ -n "$$(go fmt ./...)" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

install_errcheck:
	go get github.com/kisielk/errcheck

install_go_vet:
	go get golang.org/x/tools/cmd/vet
