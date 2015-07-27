# Kafka-Pixy

[![Build Status](https://travis-ci.org/mailgun/kafka-pixy.svg?branch=master)](https://travis-ci.org/mailgun/kafka-pixy)

Kafka-Pixy is a local aggregating HTTP proxy to Kafka messaging cluster. It is
designed to hide the complexity of the Kafka client protocol and provide a
stupid simple HTTP API that is trivial to implement in any language.

## Aggregation
Kafka works best when messages are read/written in batches, but from application
standpoint it is easier to deal with individual message read/writes. Kafka-Pixy
provides message based API to clients, but internally it collects them in
batches and submits them the way Kafka likes it the best. This behavior plays
very well with the microservices architecture, where there are usually many tiny
assorted service instances running on one beefy physical host. So Kafka-Pixy
installed on that host would aggregate messages from all the service instances,
herby effectively using the network bandwidth.

## Locality
Kafka-Pixy is intended to run on the same host as the applications using it.
Remember that it provides only message based API - no batching, therefore using
it over network is suboptimal. To encourage local usage Kafka-Pixy binds to
the Unix Domain Socket only by default. User has an option to enable a TCP
listener but that is mostly for debugging purposes.

## HTTP API

### Produce

`POST /topics/<topic>/messages?key=<key>` - submits a message to the topic with
name **topic**, using a hash of **key** to determine the shard where the message
should go. The content type can be either `text/plain` or `application/json`. 

By default a message is submitted to Kafka asynchronously, that is the HTTP
request completes as soon as the proxy gets the message, and actual message
submission to Kafka is performed afterwards. In this case the successful
completion of the HTTP request does not guarantee that the message will ever
get into Kafka, although the proxy will do its best to make that happen (retry
several times and such). If delivery guarantee is of priority over request
execution time, then synchronous delivery may be requested specifying the
**sync** parameter (exact value does not matter). In this case the request
blocks until the message is submitted to all in-sync replicas
(see [Kafka Documentation](http://kafka.apache.org/documentation.html) search
for to "Availability and Durability Guarantees"). If the returned HTTP
status code is anything but 200 OK then the message has not been submitted to
Kafka and the response body contains the error details.

If **key** is not specified then the message is submitted to a random shard.
Note that it is not the same as specifying an empty key value, for empty string
is a valid key value, and therefore all messages with an empty key value go to
the same shard.

E.g. if a Kafka-Pixy processes has been started with the `--tcpAddr=0.0.0.0:8080`
argument, then you can test it using **curl** as follows:

```
curl -X POST localhost:8080/topics/foo/messages?key=bar&sync \
     -H 'Content-Type: application/json' \
     -d '{"bar": "bazz"}'
```

## Command Line

Kafa-pixy accepts the following command line parameters:

 Parameter | Default                  | Description
-----------|--------------------------|----------------------------------------
 brokers   | localhost:9092           | Comma separated list of Kafka brokers. Note that these are just seed brokers. The rest brokers are discovered automatically.
 unixAddr  | /var/run/kafka-pixy.sock | Unix Domain Socket that the primary HTTP API should listen on.
 tcpAddr   |                          | TCP interface where the secondary HTTP API should listen. If not specified then Kafka-Pixy won't listen on a TCP socket.
 pidFile   | /var/run/kafka-pixy.pid  | Name of the pid file to create.
