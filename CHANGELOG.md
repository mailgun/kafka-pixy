# Changelog

#### Version 0.17.0 (TBD)
Implemented:
* Default client ID was changed to `kp_<hostname>_<contain-id>` if running in a
  docker container, otherwise `kp_<hostname>_<pid>`. If hostname cannot be
  retrieved, then the client ID is `kp_<random-token>`.  

#### Version 0.16.0 (2018-11-23)

Implemented:
* [#156](https://github.com/mailgun/kafka-pixy/pull/156) 
  [#151](https://github.com/mailgun/kafka-pixy/pull/151) Added formal support
  for Kafka versions up to v2.1.0.
* [#155](https://github.com/mailgun/kafka-pixy/pull/155) When the last group
  member leaves a consumer group, all group records are removed from ZooKeeper.
  Group records were never deleted from ZooKeeper before, that could cause
  issues with disposable groups.
* [#144](https://github.com/mailgun/kafka-pixy/pull/144) Added support for
  specifying and receiving Kafka record headers over both the HTTP and gRPC
  produce and consume interfaces.
* [#146](https://github.com/mailgun/kafka-pixy/pull/146) Added new configuration
  flags for the producer partitioner, broker-enforced produce timeout, and
  network-level timeouts.

Fixed:
* [#54](https://github.com/mailgun/kafka-pixy/issues/54) Rebalancing fails due 
  to non existent topic.

#### Version 0.15.0 (2018-03-30)

Implemented:
* [#135](https://github.com/mailgun/kafka-pixy/issues/135) Added support for
  Kafka v0.11.0.0 - v1.0.0
* Consumer.RebalanceTimeout was removed, so rebalancing is triggered as soon
  as membership status of a consumer group or subscription of a consumer group
  member changes.
* [#138](https://github.com/mailgun/kafka-pixy/issues/138) Pending offsets are
  committed to Kafka faster on rebalancing. That could take upto
  `consumer.offsets_commit_interval` before, but now it happens as soon as
  possible.

Fixed:
* [#120](https://github.com/mailgun/kafka-pixy/issues/120) Consumption from a
  topic stopped for a group.
* [#123](https://github.com/mailgun/kafka-pixy/issues/123) Inexplicable offset
  manager timeouts.
* [#124](https://github.com/mailgun/kafka-pixy/issues/124) Subscription to a 
  topic fails indefinitely after ZooKeeper connection loss.
* [#140](https://github.com/mailgun/kafka-pixy/issues/140) Offset manager keeps
  loosing connection with a broker.

#### Version 0.14.0 (2017-09-11)

Implemented:
* Added HTTP API endpoints to list topics optionally with partitions and
  configuration.
* Ack timeout can now be greater then subscription timeout. So in absence of
  incoming consume requests for a topic, rebalancing may be delayed until
  ack timeout expires for all offered messages. If a new request comes while
  waiting for ack timeout to expire, then the subscription timer is reset for
  the topic.
* Posts can now be performed with content type `x-www-form-urlencoded`, in that
  case message should be passed in the `msg` form parameter.
* Structural logging with sirupsen/logrus and mailgun/logrus-hooks/kafkahook.
* Support for Kafka version 0.10.2.0.

Fixed:
* Explicit acks were fixed for HTTP API. Turned out values of `noAck`,
  `ackPartition`, and `ackOffset` parameters had been ignored.
* A race was found in tests that if a request comes while a topic expiration
  is in progress consumption from the topic may never resume.
* [#100](https://github.com/mailgun/kafka-pixy/issues/100) Consumption from a
  partition stops if the segment that we read from expires.

#### Version 0.13.0 (2017-03-22)

Implemented:
* At-Least-Once delivery guarantee via synchronous production and
  explicit acknowledgement of consumed messages.
* Support for Kafka version 0.10.1.0.

#### Version 0.12.0 (2017-02-21)

Implemented:
* [#81](https://github.com/mailgun/kafka-pixy/pull/81) Added capability
  to proxy to multiple Kafka/ZooKeeper clusters.
* [#16](https://github.com/mailgun/kafka-pixy/issues/16) A YAML
  configuration file can be passed to Kafka-Pixy with `--config` command
  line parameter. A default configuration file is provided for reference.
* [#87](https://github.com/mailgun/kafka-pixy/pull/87) Added support for
  gRPC API.

Fixed:
* [#83](https://github.com/mailgun/kafka-pixy/issues/83) Panic in
  partition multiplexer.
* [#85](https://github.com/mailgun/kafka-pixy/pull/85) Another panic in
  partition multiplexer.

#### Version 0.11.1 (2016-08-11)

Bug fix release.
* [#64](https://github.com/mailgun/kafka-pixy/issues/64) Panic in group
  consumer.
* [#66](https://github.com/mailgun/kafka-pixy/issues/66) Group consumer
  closed while rebalancing in progress.
* [#67](https://github.com/mailgun/kafka-pixy/issues/67) Request timeout
  errors logged by offset manager.

#### Version 0.11.0 (2016-05-03)

Major overhaul and refactoring of the implementation to make it easier to
understand how the internal components interact with each other. It is an
important step before implementation of explicit acknowledgements can be
started.

During refactoring the following bugs were detected and fixed:
* [#56](https://github.com/mailgun/kafka-pixy/issues/56) Invalid stored
  offset makes consumer panic.
* [#59](https://github.com/mailgun/kafka-pixy/issues/59) Messages are
  skipped by consumer during rebalancing.
* [#62](https://github.com/mailgun/kafka-pixy/issues/62) Messages consumed
  twice during rebalancing.

#### Version 0.10.1 (2015-12-21)

* [#49](https://github.com/mailgun/kafka-pixy/pull/49) Topic consumption stops while ownership retained.

#### Version 0.10.0 (2015-12-16)

* [#47](https://github.com/mailgun/kafka-pixy/pull/47) Support for Kafka 0.9.0.0.
  Note that consumer group management is still implemented via ZooKeeper rather
  than the new Kafka [group management API](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-GroupMembershipAPI).

#### Version 0.9.1 (2015-11-30)

This release aims to make getting started with Kafka-Pixy easier.

* [#39](https://github.com/mailgun/kafka-pixy/issues/39) First time consume
  from a group may skip messages.
* [#40](https://github.com/mailgun/kafka-pixy/issues/40) Make output of
  `GET /topics/<>/consumers` easier to read.
* By default services listens on 19092 port for incoming API requests and a
  unix domain socket is activated only if the `--unixAddr` command line
  parameter is specified. 
* By default a pid file is not created anymore. You need to explicitly specify
  `--pidFile` command line argument to get it created.
* The source code was refactored into packages to make it easier to understand
  internal dependencies.
* Integrated with coveralls.io to collect test coverage data.

#### Version 0.8.1 (2015-10-24)

The very first version actually used in production.
