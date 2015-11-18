package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/pixy/prettyfmt"
)

const (
	reportingPeriod = 1 * time.Second
)

var (
	pixyAddr string
	topic    string
	isSync   bool
	threads  int
	count    int
	size     int
)

func init() {
	flag.StringVar(&pixyAddr, "addr", "localhost:19092", "either unix domain socker or TCP address")
	flag.StringVar(&topic, "topic", "test", "the name of the topic")
	flag.BoolVar(&isSync, "sync", false, "should production be synchronous")
	flag.IntVar(&threads, "threads", 1, "number of concurrent producer threads")
	flag.IntVar(&count, "count", 10000, "number of messages to produce")
	flag.IntVar(&size, "size", 1000, "message size in bytes")
	flag.Parse()
}

func main() {
	msg := genmessage(size)
	progressCh := make(chan int)

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

				recentProgress := 0
				checkpoint := time.Now()
				for i := messageIndexBegin; i < messageIndexEnd; i++ {
					var URL string
					if isSync {
						URL = fmt.Sprintf("%s/topics/%s/messages?sync&key=%d", baseURL, topic, i)
					} else {
						URL = fmt.Sprintf("%s/topics/%s/messages?key=%d", baseURL, topic, i)
					}
					res, err := clt.Post(URL, "text/plain", bytes.NewReader(msg))
					if err != nil {
						panic(fmt.Errorf("failed to POST %s, err=(%s)", URL, err))
					}
					if res.StatusCode != http.StatusOK {
						body, err := ioutil.ReadAll(res.Body)
						if err != nil {
							panic(err)
						}
						panic(fmt.Sprintf("%d %s", res.StatusCode, body))
					}
					res.Body.Close()
					recentProgress += 1
					if time.Now().Sub(checkpoint) > reportingPeriod {
						progressCh <- recentProgress
						recentProgress = 0
					}
				}
				progressCh <- recentProgress
			}()
		}
		wg.Wait()
		close(progressCh)
	}()

	fmt.Printf("Producing...")
	begin := time.Now()
	totalProgress := 0
	for progress := range progressCh {
		totalProgress += progress
		took := time.Now().Sub(begin)
		tookSec := float64(took) / float64(time.Second)
		fmt.Printf("\rProducing... %d/%d for %s at %dmsg(%s)/sec",
			totalProgress, count, took, int64(float64(totalProgress)/tookSec),
			prettyfmt.Bytes(int64(float64(size*totalProgress)/tookSec)))
	}
	took := time.Now().Sub(begin)
	tookSec := float64(took) / float64(time.Second)
	fmt.Printf("\rProduced %d messages of size %d for %s at %dmsg(%s)/sec\n",
		totalProgress, size, took, int64(float64(totalProgress)/tookSec),
		prettyfmt.Bytes(int64(float64(size*totalProgress)/tookSec)))
}

func genmessage(size int) []byte {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		panic(fmt.Sprintf("failed to generate message: err=(%s)", err))
	}
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if n, err := encoder.Write(raw); err != nil || n != len(raw) {
		panic(fmt.Sprintf("failed to encode message: err=(%s)", err))
	}
	encoded := buf.Bytes()
	if len(encoded) > size {
		return encoded[:size]
	}
	return encoded
}
