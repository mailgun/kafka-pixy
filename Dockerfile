FROM golang:1.13.14-alpine3.11 AS builder
RUN mkdir -p /go/src/github.com/mailgun/kafka-pixy
COPY . /go/src/github.com/mailgun/kafka-pixy
WORKDIR /go/src/github.com/mailgun/kafka-pixy
RUN apk add build-base
RUN go mod download 
RUN go build -v -o /go/bin/kafka-pixy

FROM alpine:3.11
LABEL maintainer="Maxim Vladimirskiy <horkhe@gmail.com>"
COPY --from=builder /go/bin/kafka-pixy /usr/bin/kafka-pixy
EXPOSE 19091 19092
ENTRYPOINT ["/usr/bin/kafka-pixy"]
