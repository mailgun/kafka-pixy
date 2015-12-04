# Changelog

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
