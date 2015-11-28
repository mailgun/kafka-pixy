# convenience command to update and re-vendor all dependencies
godep:
	godep update ...
	godep save -r ./...

test:
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/admin -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/config -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/consumer -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/logging -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/prettyfmt -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/producer -check.vv
	go test -v -timeout 3m github.com/mailgun/kafka-pixy/service -check.vv

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
