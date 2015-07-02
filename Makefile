# convenience command to update and re-vendor all dependencies
godep:
	godep update ...
	godep save -r ./...

test:
	go test -v ./... -cover -check.v

rebuild:
	go clean -i
	go build

all:
	go install github.com/mailgun/kafka-pixy

vet: install_go_vet
	go vet ./...

errcheck: install_errcheck
	errcheck github.com/mailgun/sarama/...

fmt:
	@if [ -n "$$(go fmt ./...)" ]; then echo 'Please run go fmt on your code.' && exit 1; fi

install_errcheck:
	go get github.com/kisielk/errcheck

install_go_vet:
	go get golang.org/x/tools/cmd/vet
