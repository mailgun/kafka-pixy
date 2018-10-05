Note: HTTP API is only provided to test Kafka-Pixy from command line and use
      in use in operations (consumer/offset API). To produce/consume messages
      please use [gRPC](http://www.grpc.io/docs/) API. You can use pre-generated
      client stubs shipped with kafka-pixy for [Python](gen/python) and
      [Golang](gen/golang), or generate them yourself from [kafkapixy.proto](kafkapixy.proto).

This tutorial assumes that topic `foo` exists in your Kafka cluster or your
Kafka is configured to create topics on demand.

To make sure that you will be able to consume the first message you produce in
the scope of this tutorial we need to start by making a consume call from a
consumer group (e.g. `bar`): 

```
curl -G localhost:19092/topics/foo/messages?group=bar
```

The call is exected to fail after the configured configured
[long polling timeout](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L109)
elapses. But the important side effect is that initial offsets will be stored
in the Kafka cluster for the used consumer group.

Note: You can use any consumer group name, but it has to be used consistently
      in all calls.

A message can be produced byt the following call:

```
curl -X POST localhost:19092/topics/foo/messages?sync \
  -d msg='May the Force be with you!'
```

The message was produced in `sync` mode, that by [default](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L70-L78)
means that Kafka-Pixy would wait for all ISR brokers to commit the message
before replying with success. That also ensures that partition and offset
that the message was committed to are returned in response. E.g.:

```json
{
  "partition": 7,
  "offset": 974563
}
```

To consume earlier produced message call:

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

The `key` is null in our case because we did not specify one when the message
was produced and therefore the partition the message was committed to had been
selected randomly.
