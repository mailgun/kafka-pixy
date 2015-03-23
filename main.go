package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/pixy"
)

const (
	defaultBrokers  = "localhost:9092"
	defaultUnixAddr = "/var/run/kafka-pixy.sock"
	defaultPIDFile  = "/var/run/kafka-pixy.pid"
)

var (
	cfg     pixy.ServiceCfg
	pidFile string
)

func init() {
	flag.StringVar(&cfg.UnixAddr, "unixAddr", defaultUnixAddr,
		"Unix domain socket address that the HTTP API should listen on")
	flag.StringVar(&cfg.TCPAddr, "tcpAddr", "",
		"TCP address that the HTTP API should listen on")
	b := flag.String("brokers", defaultBrokers, "Comma separated list of brokers")
	cfg.BrokerAddrs = strings.Split(*b, ",")
	flag.StringVar(&pidFile, "pidFile", defaultPIDFile, "Path to the PID file")
}

func main() {
	log.Init([]*log.LogConfig{&log.LogConfig{Name: "syslog"}})

	flag.Parse()

	if err := writePID(pidFile); err != nil {
		log.Errorf("Failed to write PID file, cause=(%v)", err)
		os.Exit(1)
	}

	svc, err := pixy.SpawnService(&cfg)
	if err != nil {
		log.Errorf("Failed to start service, cause=(%v)", err)
		os.Exit(1)
	}

	// Spawn OS signal listener to ensure graceful stop.
	osSigCh := make(chan os.Signal)
	signal.Notify(osSigCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		<-osSigCh
		svc.Stop()
	}()

	svc.Wait4Stop()
}

func writePID(path string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(path, []byte(fmt.Sprint(pid)), 0644)
}
