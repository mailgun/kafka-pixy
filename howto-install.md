This instruction assumes that you are trying it on a Linux host, but it will be
pretty much the same on Mac.

The easiest way to install Kafka-Pixy is to download and unpack a release
archive:

```
curl -L https://github.com/mailgun/kafka-pixy/releases/download/v0.14.0/kafka-pixy-v0.14.0-linux-amd64.tar.gz | tar xz
```

Create a configuration file using `default.yaml` as a template: 

```
cd kafka-pixy-v0.14.0-linux-amd64
cp default.yaml config.yaml
```

The default settings in the config file should be good enough for you to give
Kafka-Pixy a try, you can always fine turn it later. But for the first time you
need to at least point it to your Kafka and Zookeeper clusters.

When the config file is updated you can start Kafka-Pixy:

```
./kafka-pixy --config config.yaml
```
