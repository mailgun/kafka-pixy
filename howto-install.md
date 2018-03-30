This instruction assumes that you are trying it on a Linux host, but it will be
pretty much the same on Mac.

The easiest way to install Kafka-Pixy is to download its docker image:

```
docker pull mailgun/kafka-pixy
```

Create a configuration file using [default.yaml](https://github.com/mailgun/kafka-pixy/blob/master/default.yaml)
as a template. The default settings in the config file should be good enough
for you to give Kafka-Pixy a try, you can always fine turn it later. But for
the first time you need to at least point it to your Kafka and Zookeeper
clusters.

Then you can start it:

```
docker run -d -p 19091:19091 -p 19092:19092 -v $CONFIG_PATH:/etc/kafka-pixy.yaml mailgun/kafka-pixy --config /etc/kafka-pixy.yaml
```
