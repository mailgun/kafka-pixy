package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/logging"
	"github.com/mailgun/kafka-pixy/service"
	"github.com/mailgun/log"
)

const (
	defaultKafkaPeers     = "localhost:9092"
	defaultZookeeperPeers = "localhost:2181"
	defaultTCPAddr        = "0.0.0.0:19092"
	defaultLoggingCfg     = `[{"name": "console", "severity": "info"}]`
)

var (
	cfg            *config.T
	pidFile        string
	loggingJSONCfg string
)

func init() {
	cfg = config.Default()
	var kafkaPeers, zookeeperPeers string

	flag.StringVar(&cfg.TCPAddr, "tcpAddr", defaultTCPAddr, "TCP address that the HTTP API should listen on")
	flag.StringVar(&cfg.UnixAddr, "unixAddr", "", "Unix domain socket address that the HTTP API should listen on")
	flag.StringVar(&kafkaPeers, "kafkaPeers", defaultKafkaPeers, "Comma separated list of brokers")
	flag.StringVar(&zookeeperPeers, "zookeeperPeers", defaultZookeeperPeers, "Comma separated list of ZooKeeper nodes followed by optional chroot")
	flag.StringVar(&pidFile, "pidFile", "", "Path to the PID file")
	flag.StringVar(&loggingJSONCfg, "logging", defaultLoggingCfg, "Logging configuration")
	flag.Parse()

	cfg.Kafka.SeedPeers = strings.Split(kafkaPeers, ",")

	chrootStartIdx := strings.Index(zookeeperPeers, "/")
	if chrootStartIdx >= 0 {
		cfg.ZooKeeper.SeedPeers = strings.Split(zookeeperPeers[:chrootStartIdx], ",")
		cfg.ZooKeeper.Chroot = zookeeperPeers[chrootStartIdx:]
	} else {
		cfg.ZooKeeper.SeedPeers = strings.Split(zookeeperPeers, ",")
	}
}

func main() {
	// Make go runtime execute in parallel as many goroutines as there are CPUs.
	runtime.GOMAXPROCS(runtime.NumCPU())

	if err := initLogging(); err != nil {
		fmt.Printf("Failed to initialize logger: err=(%s)\n", err)
		os.Exit(1)
	}

	if pidFile != "" {
		if err := writePID(pidFile); err != nil {
			log.Errorf("Failed to write PID file: err=(%s)", err)
			os.Exit(1)
		}
	}

	// Clean up the unix domain socket file in case we failed to clean up on
	// shutdown the last time. Otherwise the service won't be able to listen
	// on this address and as a result will fail to start up.
	if cfg.UnixAddr != "" {
		if err := os.Remove(cfg.UnixAddr); err != nil && !os.IsNotExist(err) {
			log.Errorf("Cannot remove %s: err=(%s)", cfg.UnixAddr, err)
		}
	}

	log.Infof("Starting with config: %+v", cfg)
	svc, err := service.Spawn(cfg)
	if err != nil {
		log.Errorf("Failed to start service: err=(%s)", err)
		os.Exit(1)
	}

	// Spawn OS signal listener to ensure graceful stop.
	osSigCh := make(chan os.Signal, 1)
	signal.Notify(osSigCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// Wait for a quit signal and terminate the service when it is received.
	<-osSigCh
	svc.Stop()
}

func initLogging() error {
	var loggingCfg []log.Config
	if err := json.Unmarshal([]byte(loggingJSONCfg), &loggingCfg); err != nil {
		return fmt.Errorf("failed to parse logger config: err=(%s)", err)
	}
	if err := log.InitWithConfig(loggingCfg...); err != nil {
		return err
	}
	logging.Init3rdParty()
	return nil
}

func writePID(path string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(path, []byte(fmt.Sprint(pid)), 0644)
}
