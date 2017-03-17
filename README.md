# Kafka-Pixy (gRPC/REST Proxy for Kafka)

[![Build Status](https://travis-ci.org/mailgun/kafka-pixy.svg?branch=master)](https://travis-ci.org/mailgun/kafka-pixy) [![Go Report Card](https://goreportcard.com/badge/github.com/mailgun/kafka-pixy)](https://goreportcard.com/report/github.com/mailgun/kafka-pixy) [![Coverage Status](https://coveralls.io/repos/mailgun/kafka-pixy/badge.svg?branch=master&service=github)](https://coveralls.io/github/mailgun/kafka-pixy?branch=master)

Kafka-Pixy is a dual API (gRPC and REST) proxy for [Kafka](http://kafka.apache.org/documentation.html)
with automatic consumer group control. It is designed to hide the
complexity of the Kafka client protocol and provide a stupid simple
API that is trivial to implement in any language.

Kafka-Pixy supports Kafka **0.8.2.x** and **0.9.0.x**. It uses the
Kafka [Offset Commit/Fetch API](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI)
to keep track of consumer offsets and ZooKeeper to manage distribution
of partitions among consumer group members.

You can jump to [Quick Start](README.md#quick-start) if you are anxious
to give it a try.

#### Key Features:

- **At Least Once Guarantee**: The main feature of Kafka-Pixy is that
  it guarantees at-least-once message delivery. The guarantee is
  achieved via combination of synchronous production and explicit
  acknowledgement of consumed messages;
- **Dual API**: Kafka-Pixy provides two types of API:
  - [gRPC](http://www.grpc.io/docs/guides/)
  ([Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview)
  over [HTTP/2](https://http2.github.io/faq/)) recommended to
  produce/consume messages;
  - REST (JSON over HTTP) intended for for testing and operations
  purposes, although you can use it to produce/consume messages too;
- **Muli-Cluster Support**: One Kafka-Pixy instance can proxy to
  several Kafka clusters. You just need to define them in the [config
  file](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml)
  and then address clusters by name given in the config file in your
  API requests.
- **Aggregation**: Kafka works best when messages are read/written in
  batches, but from application standpoint it is easier to deal with
  individual message read/writes. Kafka-Pixy provides message based API
  to clients, but internally it aggregates requests and sends them to
  Kafka in batches.
- **Locality**: Kafka-Pixy is intended to run on the same host as the
  applications using it. Remember that it provides only message based
  API - no batching, therefore using it over network is suboptimal.

## gRPC API

[gRPC](http://www.grpc.io/docs/guides/) is an opens source framework
that is using [Protocol Buffers](https://developers.google.com/protocol-buffers/docs/overview)
as interface definition language and [HTTP/2](https://http2.github.io/faq/)
as transport protocol. Kafka-Pixy API is defined in
[grpc.proto](https://github.com/mailgun/kafka-pixy/blob/master/grpc.proto).
Client stubs for [Golang](https://github.com/mailgun/kafka-pixy/blob/master/gen/golang)
and [Python](https://github.com/mailgun/kafka-pixy/tree/master/gen/python)
are generated and provided in this repository, but you can easily
generate stubs for a bunch of other languages. Please refer to the gRPC
[documentation](http://www.grpc.io/docs/) for information on the
language of your choice.

## HTTP API

**It is highly recommended to use gRPC API for production/consumption.
The HTTP API is only provided for quick tests and operational purposes.**

Each API endpoint has two variants which differ by `/clusters/<cluster>`
prefix. The one with the proxy prefix is to be used when multiple
clusters are configured. The one without the prefix operates on the
default cluster (the one that is mentioned first in the YAML
configuration file).

### Produce

```
POST /topics/<topic>/messages
POST /clusters/<cluster>/topics/<topic>/messages
```

Writes a message to a topic on a particular cluster. the message should
be send as the body of the request which can be either `text/plain` or
or `application/json`.

 Parameter | Opt | Description
-----------|-----|------------------------------------------------------
 cluster   | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic     |     | The name of a topic to produce to
 key       | yes | A string that hash is used to determine a partition to produce to. By default a random partition is selected.
 sync      | yes | A flag (value is ignored) that makes Kafka-Pixy wait for all ISR to confirm write before sending a response back. By default a response is sent immediatelly after the request is received.

By default the message is written to Kafka asynchronously, that is the
HTTP request completes as soon as Kafka-Pixy reads the request from the
wire, and production to Kafka is performed on the background. Therefore
it is not guarantee that the message will ever get into Kafka. To ensure
that a request returns **200 OK** after the message is written to all
in-sync replicas pass **sync** flag in your request.

E.g. if a Kafka-Pixy process has been started with the `--tcpAddr=0.0.0.0:8080`
argument, then you can test it using **curl** as follows:

```
curl -X POST localhost:8080/topics/foo/messages?key=bar&sync \
  -H 'Content-Type: text/plain' \
  -d 'Good news everyone!'
```

If the message is submitted asynchronously then the response will be an
empty json object `{}`.
 
If the message is submitted synchronously then in case of success (HTTP
status **200**) the response will be like:

```
{
  "partition": <partition number>,
  "offset": <message offset>
}
```

In case of failure (HTTP statuses **404** and **500**) the response
will be:

```
{
  "error": <human readable explanation>
}
```

### Consume

```
GET /topics/<topic>/messages
GET /clusters/<cluster>/topics/<topic>/messages
```

Consumes a message from a topic of a particular cluster as a member of
a particular consumer group. A message previously consumed from the same
topic can be optionally acknowledged.

 Parameter    | Opt | Description
--------------|-----|------------------------------------------------------
 cluster      | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic        |     | The name of a topic to produce to.
 group        |     | The name of a consumer group.
 noAck        | yes | A flag (value is ignored) that no message should be acknowledged. For default behaviour read below.
 ackPartition | yes | A partition number that the acknowledged message was consumed from. For default behaviour read below.
 ackOffset    | yes | An offset of the acknowledged message. For default behaviour read below.

If **noAck** is defined in a request then no message is acknowledged
by the request. If a request defines both **ackPartition** and
**ackOffset** parameters then a message previously consumed from the
same topic from the specified partition with the specified offset is
acknowledged by the request. If none of the ack relates parameters is
specified then the request will acknowledge the message consumed in this
requests if any. It is called `auto-ack` mode.

When a message is consumed as a member of a consume group for the first
time, Kafka-Pixy joins the consumer group and subscribes to the topic.
All Kafka-Pixy instances that are currently members of that group and
subscribed to that topic distribute partitions between themselves, so
that each Kafka-Pixy instance gets a subset of partitions for exclusive
consumption (Read more about what the Kafka consumer groups
[here](http://kafka.apache.org/documentation.html#intro_consumers)).

If a Kafka-Pixy instance has not received consume requests for a topic for
[registration timeout](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L72),
then it unsubscribes from the topic, and the topic partitions are
redistributed among Kafka-Pixy instances that are still consuming from it.
 
If there are no unread messages in the topic the request will block
waiting for [long polling timeout](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L67).
If there are no messages produced during this long poll waiting then the request
will return **408 Request Timeout** error, otherwise the response will
be a JSON document of the following structure:

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

### Acknowledge

```
POST /topics/<topic>/messages
POST /clusters/<cluster>/topics/<topic>/messages
```

Acknowledges a previously consumed message.

 Parameter | Opt | Description
-----------|-----|------------------------------------------------------
 cluster   | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic     |     | The name of a topic to produce to.
 group     |     | The name of a consumer group.
 partition |     | A partition number that the acknowledged message was consumed from.
 offset    |     | An offset of the acknowledged message.

### Get Offsets
 
```
GET /topics/<topic>/offsets
GET /clusters/<cluster>/topics/<topic>/offsets
```

Returns offset information for all partitions of the specified **topic**
including the next offset to be consumed by the specified consumer group. The
structure of the returned JSON document is as follows:

 Parameter | Opt | Description
-----------|-----|------------------------------------------------------
 cluster   | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic     |     | The name of a topic to produce to.
 group     |     | The name of a consumer group.

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

```
POST /topics/<topic>/offsets
POST /clusters/<cluster>/topics/<topic>/offsets
```

Sets offsets to be consumed from the specified topic by a particular consumer
group. The request content should be a list of JSON objects, where each object
defines an offset to be set for a particular partition:

 Parameter | Opt | Description
-----------|-----|------------------------------------------------------
 cluster   | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic     |     | The name of a topic to produce to.
 group     |     | The name of a consumer group.

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

```
GET /topics/<topic>/consumers
GET /clusters/<topic>/topics/<topic>/consumers
```

Returns a list of consumers that are subscribed to a topic.

 Parameter | Opt | Description
-----------|-----|------------------------------------------------------
 cluster   | yes | The name of a cluster to operate on. By default the cluster mentioned first in the `proxies` section of the config file is used.
 topic     |     | The name of a topic to produce to.
 group     | yes | The name of a consumer group. By default returns data for all known consumer groups subscribed to the topic.

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

## Configuration

Kafa-Pixy is designed to be very simple to run. It consists of a single
executable that can be started just by passing a bunch of command line
parameters to it - no configuration file needed.

However if you do need to fine-tune Kafka-Pixy for your use case, you can
provide a YAML configuration file. Default configuration file
[default.yaml](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml)
is shipped in the release archive. In you configuration file you can specify
only parameters that you want to change, other options take their default
values. If some option is both specified in the configuration file and provided
as a command line argument, then the command line argument wins.

Command line parameters that Kafka-Pixy accepts are listed below:

 Parameter      | Description
----------------|-------------------------------------------------------------------
 config         | Path to a YAML configuration file.
 kafkaPeers     | Comma separated list of Kafka brokers. Note that these are just seed brokers. The rest brokers are discovered automatically. (Default **localhost:9092**)
 zookeeperPeers | Comma separated list of ZooKeeper nodes followed by optional chroot. (Default **localhost:2181**)
 grpcAddr       | TCP address that the gRPC API should listen on. (Default **0.0.0.0:19091**)
 tcpAddr        | TCP address that the HTTP API should listen on. (Default **0.0.0.0:19092**)
 unixAddr       | Unix Domain Socket that the HTTP API should listen on. If not specified then the service will not listen on a Unix Domain Socket.
 pidFile        | Name of a pid file to create. If not specified then a pid file is not created.

You can run `kafka-pixy -help` to make it list all available command line
parameters.

## Quick Start

This instruction assumes that you are trying it on Linux host, but it will be
pretty much the same on Mac.

### Step 1. Download

```
curl -L https://github.com/mailgun/kafka-pixy/releases/download/v0.12.0/kafka-pixy-v0.12.0-linux-amd64.tar.gz | tar xz
```

### Step 2. Start

```
cd kafka-pixy-v0.12.0-linux-amd64
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
