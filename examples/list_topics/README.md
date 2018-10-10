To run this example:
 1. Start vagrant box:
  `vagrant up`
 2. Build kafka-pixy:
  `go install ./...`
 3. Start kafka-pixy:
  `${GOPATH}/bin/kafka-pixy --kafkaPeers=192.168.100.67:9091 --zookeeperPeers=192.168.100.67:2181`
 4. Run it:
  `go run examples/get_topics/main.go`

