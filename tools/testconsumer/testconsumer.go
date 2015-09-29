package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/pixy"
)

const (
	reportingPeriod = 1 * time.Second
)

var (
	pixyAddr    string
	group       string
	topic       string
	threads     int
	count       int
	waitForMore bool
)

func init() {
	flag.StringVar(&pixyAddr, "addr", "/var/run/kafka-pixy.sock", "either unix domain socker or TCP address")
	flag.StringVar(&group, "group", "test", "the name of the consumer group")
	flag.StringVar(&topic, "topic", "test", "the name of the topic")
	flag.IntVar(&threads, "threads", 1, "number of concurrent producer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to consume")
	flag.BoolVar(&waitForMore, "wait", false, "should wait for more messages when the end of the topic is reached")
	flag.Parse()
}

type progress struct {
	count int
	bytes int64
}

func main() {
	progressCh := make(chan progress)

	useUnixDomainSocket := false
	var baseURL string
	if strings.HasPrefix(pixyAddr, "/") {
		fmt.Printf("Using UDS client for %s\n", pixyAddr)
		baseURL = "http://_"
		useUnixDomainSocket = true
	} else {
		fmt.Printf("Using net client for %s\n", pixyAddr)
		baseURL = fmt.Sprintf("http://%s", pixyAddr)
	}
	URL := fmt.Sprintf("%s/topics/%s/messages?group=%s", baseURL, topic, group)

	go func() {
		var wg sync.WaitGroup
		chunkSize := count / threads
		for i := 0; i < threads; i++ {
			messageIndexBegin := chunkSize * i
			messageIndexEnd := messageIndexBegin + chunkSize
			if count-messageIndexEnd < chunkSize {
				messageIndexEnd = count
			}
			wg.Add(1)
			go func() {
				defer wg.Done()

				var clt http.Client
				if useUnixDomainSocket {
					dial := func(proto, addr string) (net.Conn, error) {
						return net.Dial("unix", pixyAddr)
					}
					clt.Transport = &http.Transport{Dial: dial}
				}

				var recentProgress progress
				checkpoint := time.Now()
				for i := messageIndexBegin; i < messageIndexEnd; i++ {
					res, err := clt.Get(URL)
					if err != nil {
						panic(err)
					}
					body, err := ioutil.ReadAll(res.Body)
					if err != nil {
						panic(err)
					}
					res.Body.Close()
					if res.StatusCode == http.StatusRequestTimeout {
						if waitForMore {
							continue
						}
						break
					}
					if res.StatusCode != http.StatusOK {
						panic(fmt.Sprintf("%d %s", res.StatusCode, body))
					}
					recentProgress.count += 1
					recentProgress.bytes += int64(len(body))
					if time.Now().Sub(checkpoint) > reportingPeriod {
						progressCh <- recentProgress
						recentProgress.count, recentProgress.bytes = 0, 0
					}
				}
				progressCh <- recentProgress
			}()
		}
		wg.Wait()
		close(progressCh)
	}()

	fmt.Printf("Consuming...")
	begin := time.Now()
	var totalProgress progress
	for progress := range progressCh {
		totalProgress.count += progress.count
		totalProgress.bytes += progress.bytes
		took := time.Now().Sub(begin)
		tookSec := float64(took) / float64(time.Second)
		fmt.Printf("\rConsuming... %d(%s)/%d for %s at %dmsg(%s)/sec    ",
			totalProgress.count, pixy.BytesToStr(totalProgress.bytes), count, took,
			int64(float64(totalProgress.count)/tookSec),
			pixy.BytesToStr(int64(float64(totalProgress.bytes)/tookSec)))
	}
	took := time.Now().Sub(begin)
	tookSec := float64(took) / float64(time.Second)
	fmt.Printf("\rConsumed %d(%s)/%d for %s at %dmsg(%s)/sec        \n",
		totalProgress.count, pixy.BytesToStr(totalProgress.bytes), count, took,
		int64(float64(totalProgress.count)/tookSec),
		pixy.BytesToStr(int64(float64(totalProgress.bytes)/tookSec)))
}
