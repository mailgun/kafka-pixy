FROM golang:1.11.2-alpine3.8 AS builder
RUN mkdir -p /go/src/github.com/mailgun/kafka-pixy
COPY . /go/src/github.com/mailgun/kafka-pixy
WORKDIR /go/src/github.com/mailgun/kafka-pixy
RUN go build -v -o /go/bin/kafka-pixy

FROM alpine:3.8
LABEL maintainer="Maxim Vladimirskiy <horkhe@gmail.com>"
COPY --from=builder /go/bin/kafka-pixy /usr/bin/kafka-pixy
EXPOSE 19091 19092
ENTRYPOINT ["/usr/bin/kafka-pixy"]
