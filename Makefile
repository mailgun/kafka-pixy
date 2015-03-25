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
