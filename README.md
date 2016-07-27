# Kafka-Pixy (HTTP Proxy)

[![Build Status](https://travis-ci.org/mailgun/kafka-pixy.svg?branch=master)](https://travis-ci.org/mailgun/kafka-pixy) [![Go Report Card](http://goreportcard.com/badge/mailgun/kafka-pixy)](http://goreportcard.com/report/mailgun/kafka-pixy) [![Coverage Status](https://coveralls.io/repos/mailgun/kafka-pixy/badge.svg?branch=master&service=github)](https://coveralls.io/github/mailgun/kafka-pixy?branch=master)

Kafka-Pixy is a local aggregating HTTP proxy to [Kafka](http://kafka.apache.org/documentation.html)
with automatic consumer group control. It is designed to hide the complexity of
the Kafka client protocol and provide a stupid simple HTTP API that is trivial
to implement in any language.

Kafka-Pixy works with Kafka **0.8.2.x** and **0.9.0.x**. It uses the Kafka 
[Offset Commit/Fetch API](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI)
to keep track of consumer offsets and ZooKeeper to manage distribution of
partitions among consumer group members.

You can jump to [Quick Start](README.md#quick-start) if you are anxious to give it a try.

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
it over network is suboptimal. To make local usage even more efficient
Kafka-Pixy provides an option to serve its API via a Unix Domain Socket
alongside a TCP socket (**0.0.0.0:19092** by default).

## HTTP API

### Produce

`POST /topics/<topic>/messages?key=<key>` - submits a message to the specified
**topic** using the hash of the specified **key** to determine the partition
that the message should go to. The content type can be either `text/plain` or
`application/json`. 

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

If the message is submitted asynchronously then the response will be an empty
json object `{}`.
 
If the message is submitted synchronously then in case of success (HTTP status
**200**) the response will be like:

```
{
  "partition": <partition number>,
  "offset": <message offset>
}
```

In case of failure (HTTP statuses **404** and **500**) the response will be.

```
{
  "error": <human readable explanation>
}
```

### Consume

`GET /topics/<topic>/messages?group=<group>` - consumes a message from the
specified **topic** on behalf of the specified consumer **group**.
 
When a message is consumed on behalf of a consume group for the first time,
the Kafka-Pixy instance joins the consumer group and subscribes to the topic.
All Kafka-Pixy instances that are currently members of that group and subscribed
to that topic divide topic partitions among themselves, so that each Kafka-Pixy
instance gets a subset of partitions for exclusive consumption (Read more about
what the Kafka consumer groups [here](http://kafka.apache.org/documentation.html#intro_consumers)).
If a Kafka-Pixy instance has not consumed from a particular topic on behalf of
a particular consumer group for 20 seconds (the value is not configurable yet),
then it unsubscribes from the topic on behalf of that group, and the topic
partitions are redistributed among Kafka-Pixy instances that are still
subscribed to the topic.
 
If there are no new messages in the topic the request will block waiting for 3 seconds.
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
all partitions of the specified **topic** including the next offset to be consumed
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
be a list of JSON objects, where each object defines an offset to be set for
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

### List Consumers

`GET /topics/<topic>/consumers[?group=<group>]` - returns a list of consumers
that are subscribed to the specified **topic** along with a list of partitions
assigned to each consumer. If **group** is not specified then information is
provided for all consumer groups subscribed to the **topic**.

e.g.:

```
curl -G localhost:19092/topic/some_queue/consumers
```

yields:

```
{
  "integrations": {
    "pixy_jobs1_62065_2015-09-24T22:21:05Z": [0,1,2,3],
    "pixy_jobs2_18075_2015-09-24T22:21:28Z": [4,5,6],
    "pixy_jobs3_336_2015-09-24T22:21:51Z": [7,8,9]
  },
  "logstash-customer": {
    "logstash-customer_logs01-1443116116450-7f54d246-0": [0,1,2],
    "logstash-customer_logs01-1443116116450-7f54d246-1": [3,4,5],
    "logstash-customer_logs01-1443116116450-7f54d246-2": [6,7],
    "logstash-customer_logs01-1443116116450-7f54d246-3": [8,9]
  },
  "logstash-reputation4": {
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-0": [0],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-1": [1],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-10": [2],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-11": [3],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-12": [4],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-13": [5],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-14": [6],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-15": [7],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-2": [8],
    "logstash-reputation4_logs16-1443178335419-c08d8ab6-3": [9]
  },
  "test": {
    "pixy_core1_47288_2015-09-24T22:15:36Z": [0,1,2,3,4],
    "pixy_in7_102745_2015-09-24T22:24:14Z": [5,6,7,8,9]
  }
}
```

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
 tcpAddr        | TCP interface where the HTTP API should listen. (Default **0.0.0.0:19092**)
 unixAddr       | Unix Domain Socket that the HTTP API should listen on. If not specified then the service will not listen on a Unix Domain Socket. 
 pidFile        | Name of a pid file to create. If not specified then a pid file is not created.

You can run `kafka-pixy -help` to make it list all available command line
parameters.

## Quick Start

This instruction assumes that you are trying it on Linux host, but it will be
pretty much the same on Mac.

### Step 1. Download

```
curl -L https://github.com/mailgun/kafka-pixy/releases/download/v0.11.0/kafka-pixy-v0.11.0-1-ge927f15-linux-amd64.tar.gz | tar xz
```

### Step 2. Start

```
cd kafka-pixy-v0.11.0-linux-amd64
./kafka-pixy --kafkaPeers "<host1>:9092,...,<hostN>:9092" --zookeeperPeers "<host1>:2181,...,<hostM>:2181"
```

### Step 3. Create Topic (optional)

If your Kafka cluster is configured to require explicit creation of topics, then
create one for your testing (e.g. `foo`). [Here](http://kafka.apache.org/documentation.html#basic_ops_add_topic)
is how you can do that.

### Step 4. Initialize Group Offsets

Consume from the topic on behalf of a consumer group (e.g. `bar`) for the first
time. The consumption will fail with the long polling timeout (3 seconds), but
the important side effect of that is that initial offsets will be stored in
Kafka.

```
curl -G localhost:19092/topics/foo/messages?group=bar
```

Output:

```json
{
  "error": "long polling timeout"
}
```

### Step 5. Produce

```
curl -X POST localhost:19092/topics/foo/messages?sync \
  -H 'Content-Type: text/plain' \
  -d 'blah blah blah'
```

The output tells the partition the message has been submitted to and the offset
it has:

```json
{
  "partition": 7,
  "offset": 974563
}
```

### Step 6. Consume

```
curl -G localhost:19092/topics/foo/messages?group=bar
```

The output provides the retrieved message as a base64 encoded value along with
some metadata:

```json
{
  "key": null,
  "value": "YmxhaCBibGFoIGJsYWg=",
  "partition": 7,
  "offset": 974563
}
```

## License

Kafka-Pixy is under the Apache 2.0 license. See the [LICENSE](LICENSE) file for details.
