# Kafka-Pixy

[![Build Status](https://travis-ci.org/mailgun/kafka-pixy.svg?branch=master)](https://travis-ci.org/mailgun/kafka-pixy)

Kafka-Pixy is a local aggregating HTTP proxy to Kafka messaging cluster. It is
designed to hide the complexity of the Kafka client protocol and provide a
stupid simple HTTP API that is trivial to implement in any language.

Kafka-Pixy works with Kafka **8.2.x** or later, because it uses the
[Offset Commit/Fetch API](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI)
released in that version. Although this API has been introduced in Kafka **0.8.1.1**,
the on-the-wire packet format was different back then, and Kafka-Pixy does not
support the older format.

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

### Consume

`GET /topics/<topic>/messages?group=<group>` - consumes a message from the topic
with name **topic** on behalf of a consumer group with name **group**. If there
are no new messages in the topic the request will block waiting for 3 seconds.
If there are no messages produced during this long poll waiting then the request
will return **408** Request Timeout error, otherwise the response will be a JSON
document of the following structure:

```
{
    "key": <base64 encoded key>,
    "value": <base64 encoded message body>,
    "partition": <partition number>,
    "offset": <message offset>
}
```
e.g.:
```json
{
    "key": "0JzQsNGA0YPRgdGP",
    "value": "0JzQvtGPINC70Y7QsdC40LzQsNGPINC00L7Rh9C10L3RjNC60LA=",
    "partition": 0,
    "offset": 13}
}
```

### Get Offsets
 
`GET /topics/<topic>/offsets?group=<group>` - returns offset information for
all partitions of the specified topic including the next offset to be consumed
by the specified consumer group. The structure of the returned JSON document is
as follows:

```
[
  {
    "partition": <partition id>,
    "begin": <oldest offset>,
    "end": <newest offset>,
    "count": <the number of messages in the topic, equals to `end` - `begin`>,
    "offset": <next offset to be consumed by this consumer group>,
    "lag": <equals to `end` - `offset`>,
    "metadata": <arbitrary string committed with the offset, not used by Kafka-Pixy. It is omitted if empty>
  },
  ...
]
```

### Set Offsets

`POST /topics/<topic>/offsets?group=<group>` - sets offsets to be consumed from
the specified topic by a particular consumer group. The request content should
be a list of JSON objects, where each objects defines an offset to be set for
a particular partition:

```
[
  {
    "partition": <partition id>,
    "offset": <next offset to be consumed by this consumer group>,
    "metadata": <arbitrary string>
  },
  ...
]
```

Note that consumption by all consumer group members should cease before this
call can be executed. That is necessary because while consuming Kafka-Pixy
constantly updates partition offsets, and it does not expect them to be update
by somebody else. So it only reads them on group initialization, that happens
when a consumer group request comes after 20 seconds or more of the consumer
group inactivity on all Kafka-Pixy working with the Kafka cluster.

## Delivery Guarantees

If a Kafka-Pixy instance dies (crashes or gets brutally killed with SIGKILL, or
entire host running a Kafka-Pixy instance goes down, you name it), then
some **asynchronously** produced messages can be lost, but **synchronously**
produced messages are never lost. Some messages consumed just before the death
can be consumed for the second time later either from the restarted Kafka-Pixy
instance on the same host or a Kafka-Pixy instance running on another host.

A message is considered to be consumed by Kafka-Pixy if it is successfully sent
over network in an HTTP response body. So if a client application dies before
the message is processed, then it will be lost. 

## Command Line

Kafa-Pixy is designed to be very simple to run. It consists of a single
executable that can be started just by passing a bunch of command line
parameters to it - no configuration file needed. All configuration parameters
that Kafka-Pixy accepts are listed below.

 Parameter      | Description
----------------|-------------------------------------------------------------------
 kafkaPeers     | Comma separated list of Kafka brokers. Note that these are just seed brokers. The rest brokers are discovered automatically. (Default **localhost:9092**)
 zookeeperPeers | Comma separated list of ZooKeeper nodes followed by optional chroot. (Default **localhost:2181**)
 unixAddr       | Unix Domain Socket that the primary HTTP API should listen on. (Default **/var/run/kafka-pixy.sock**)
 tcpAddr        | TCP interface where the secondary HTTP API should listen. If not specified then Kafka-Pixy won't listen on a TCP socket.
 pidFile        | Name of the pid file to create. (Default **/var/run/kafka-pixy.pid**)

You can run `kafka-pixy -help` to make it list all available command line
parameters.
