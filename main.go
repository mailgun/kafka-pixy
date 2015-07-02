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

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/pixy"
)

const (
	defaultBrokers    = "localhost:9092"
	defaultUnixAddr   = "/var/run/kafka-pixy.sock"
	defaultPIDFile    = "/var/run/kafka-pixy.pid"
	defaultLoggingCfg = `[{"name": "console", "severity": "info"}]`
)

var (
	serviceCfg     pixy.ServiceCfg
	pidFile        string
	loggingJSONCfg string
)

func init() {
	flag.StringVar(&serviceCfg.UnixAddr, "unixAddr", defaultUnixAddr,
		"Unix domain socket address that the HTTP API should listen on")
	flag.StringVar(&serviceCfg.TCPAddr, "tcpAddr", "",
		"TCP address that the HTTP API should listen on")
	b := flag.String("brokers", defaultBrokers, "Comma separated list of brokers")
	flag.StringVar(&pidFile, "pidFile", defaultPIDFile, "Path to the PID file")
	flag.StringVar(&loggingJSONCfg, "logging", defaultLoggingCfg, "Logging configuration")
	flag.Parse()
	serviceCfg.BrokerAddrs = strings.Split(*b, ",")
}

func main() {
	// Make go runtime execute in parallel as many goroutines as there are CPUs.
	runtime.GOMAXPROCS(runtime.NumCPU())

	if err := initLogging(); err != nil {
		fmt.Printf("Failed to initialize logger, cause=(%v)\n", err)
		os.Exit(1)
	}

	if err := writePID(pidFile); err != nil {
		log.Errorf("Failed to write PID file, cause=(%v)", err)
		os.Exit(1)
	}

	// Clean up the unix domain socket file in case we failed to clean up on
	// shutdown the last time. Otherwise the service won't be able to listen
	// on this address and the service will terminated immediately.
	if err := os.Remove(serviceCfg.UnixAddr); err != nil && err.(*os.PathError).Err != os.ErrNotExist {
		log.Errorf("Cannot remove %s", serviceCfg.UnixAddr)
	}

	log.Infof("Starting with config: %+v", serviceCfg)
	svc, err := pixy.SpawnService(&serviceCfg)
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

func initLogging() error {
	var loggingCfg []log.Config
	if err := json.Unmarshal([]byte(loggingJSONCfg), &loggingCfg); err != nil {
		return fmt.Errorf("failed to parse logger config, cause=(%v)", err)
	}
	if err := log.InitWithConfig(loggingCfg...); err != nil {
		return err
	}
	return nil
}

func writePID(path string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(path, []byte(fmt.Sprint(pid)), 0644)
}
