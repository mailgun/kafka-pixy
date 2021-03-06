package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/mailgun/holster/v4/setter"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/logging"
	"github.com/mailgun/kafka-pixy/service"
	log "github.com/sirupsen/logrus"
)

var (
	cmdGRPCAddr          string
	cmdConfig            string
	cmdTCPAddr           string
	cmdUnixAddr          string
	cmdKafkaPeers        string
	cmdZookeeperPeers    string
	cmdPIDFile           string
	cmdLoggingJSONCfg    string
	cmdTLSEnabled        bool
	cmdCACertFile        string
	cmdClientCertFile    string
	cmdClientCertKeyFile string
	cmdInsecure          bool
)

func init() {
	flag.StringVar(&cmdConfig, "config", "", "YAML configuration file, refer to https://github.com/mailgun/kafka-pixy/blob/master/default.yaml for a list of available configuration options")
	flag.StringVar(&cmdGRPCAddr, "grpcAddr", "", "TCP address that the gRPC API should listen on")
	flag.StringVar(&cmdTCPAddr, "tcpAddr", "", "TCP address that the HTTP API should listen on")
	flag.StringVar(&cmdUnixAddr, "unixAddr", "", "Unix domain socket address that the HTTP API should listen on")
	flag.StringVar(&cmdKafkaPeers, "kafkaPeers", "", "Comma separated list of brokers")
	flag.StringVar(&cmdZookeeperPeers, "zookeeperPeers", "", "Comma separated list of ZooKeeper nodes followed by optional chroot")
	flag.StringVar(&cmdPIDFile, "pidFile", "", "Path to the PID file")
	flag.StringVar(&cmdLoggingJSONCfg, "logging", "", "Logging configuration")
	flag.BoolVar(&cmdTLSEnabled, "tls", false, "Enable TLS (Kafka consumer/producer)")
	flag.StringVar(&cmdCACertFile, "caCertFile", "", "CA certificate file")
	flag.StringVar(&cmdClientCertFile, "clientCertFile", "", "Client certificate file")
	flag.StringVar(&cmdClientCertKeyFile, "clientCertKeyFile", "", "Client certificate key file")
	flag.BoolVar(&cmdInsecure, "insecureTLS", false, "Disable TLS hostname verification")
	flag.Parse()
}

func main() {
	cfg, err := makeConfig()
	if err != nil {
		fmt.Printf("Failed to load config: err=(%s)\n", err)
		os.Exit(1)
	}

	if err := logging.Init(cmdLoggingJSONCfg, cfg); err != nil {
		fmt.Printf("Failed to initialize logger: err=(%s)\n", err)
		os.Exit(1)
	}

	if cmdPIDFile != "" {
		if err := writePID(cmdPIDFile); err != nil {
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

func makeConfig() (*config.App, error) {
	var cfg *config.App
	// If a YAML configuration file is provided, then load it and let
	// parameters provided on the command line override values on it.
	if cmdConfig != "" {
		var err error
		if cfg, err = config.FromYAMLFile(cmdConfig); err != nil {
			return nil, err
		}
	} else {
		cfg = config.DefaultApp("default")
	}

	if cmdGRPCAddr != "" {
		cfg.GRPCAddr = cmdGRPCAddr
	}
	if cmdTCPAddr != "" {
		cfg.TCPAddr = cmdTCPAddr
	}
	if cmdUnixAddr != "" {
		cfg.UnixAddr = cmdUnixAddr
	}
	if cmdKafkaPeers != "" {
		cfg.Proxies[cfg.DefaultCluster].Kafka.SeedPeers = strings.Split(cmdKafkaPeers, ",")
	}
	if cmdTLSEnabled {
		cfg.Proxies[cfg.DefaultCluster].Kafka.TLSEnabled = cmdTLSEnabled
	}
	if cmdCACertFile != "" {
		cfg.Proxies[cfg.DefaultCluster].Kafka.CACertFile = cmdCACertFile
	}
	if cmdClientCertFile != "" {
		cfg.Proxies[cfg.DefaultCluster].Kafka.ClientCertFile = cmdClientCertFile
	}
	if cmdClientCertKeyFile != "" {
		cfg.Proxies[cfg.DefaultCluster].Kafka.ClientCertKeyFile = cmdClientCertKeyFile
	}
	if cmdInsecure {
		cfg.Proxies[cfg.DefaultCluster].Kafka.InsecureSkipVerify = cmdInsecure
	}
	if cmdZookeeperPeers != "" {
		chrootStartIdx := strings.Index(cmdZookeeperPeers, "/")
		if chrootStartIdx >= 0 {
			cfg.Proxies[cfg.DefaultCluster].ZooKeeper.SeedPeers = strings.Split(cmdZookeeperPeers[:chrootStartIdx], ",")
			cfg.Proxies[cfg.DefaultCluster].ZooKeeper.Chroot = cmdZookeeperPeers[chrootStartIdx:]
		} else {
			cfg.Proxies[cfg.DefaultCluster].ZooKeeper.SeedPeers = strings.Split(cmdZookeeperPeers, ",")
		}
	}
	setter.SetDefault(&cfg.Logging, []config.LoggerCfg{
		{
			Name:     "console",
			Severity: "info",
		},
	})
	return cfg, nil
}

func writePID(path string) error {
	pid := os.Getpid()
	return ioutil.WriteFile(path, []byte(fmt.Sprint(pid)), 0644)
}
