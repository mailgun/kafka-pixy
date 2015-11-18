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

	"github.com/mailgun/kafka-pixy/pixy/prettyfmt"
)

const (
	reportingPeriod    = 5 * time.Second
	backOffTimeout     = 3 * time.Second
	longPollingTimeout = 4 * time.Second
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
	flag.StringVar(&pixyAddr, "addr", "localhost:19092", "either unix domain socker or TCP address")
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
	mf := SpawnMessageFetcher(pixyAddr, topic, group)
	progressCh := make(chan progress)
	go func() {
		var wg sync.WaitGroup
		chunkSize := count / threads
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				var recentProgress progress
				checkpoint := time.Now()
				for j := 1; j < chunkSize; j++ {
					select {
					case msg, ok := <-mf.Messages():
						if !ok {
							goto done
						}

						recentProgress.count += 1
						recentProgress.bytes += int64(len(msg))
						now := time.Now()
						if now.Sub(checkpoint) > reportingPeriod {
							checkpoint = now
							progressCh <- recentProgress
							recentProgress.count, recentProgress.bytes = 0, 0
						}
					case <-time.After(longPollingTimeout):
						if waitForMore {
							continue
						}
						goto done
					}
				}
			done:
				progressCh <- recentProgress
			}()
		}
		wg.Wait()

		go func() {
			mf.Stop()
		}()
		// Read the remaining fetched but not processed messages.
		var lastProgress progress
		for msg := range mf.Messages() {
			lastProgress.bytes = int64(len(msg))
			lastProgress.count += 1
		}
		progressCh <- lastProgress
		close(progressCh)
	}()

	begin := time.Now()
	checkpoint := begin
	var totalProgress progress
	for progress := range progressCh {
		totalProgress.count += progress.count
		totalProgress.bytes += progress.bytes
		now := time.Now()
		totalTook := now.Sub(begin)
		took := now.Sub(checkpoint)
		checkpoint = now
		tookSec := float64(took) / float64(time.Second)
		fmt.Printf("Consuming... %d(%s) for %s at %dmsg(%s)/sec\n",
			totalProgress.count, prettyfmt.Bytes(totalProgress.bytes), totalTook,
			int64(float64(progress.count)/tookSec),
			prettyfmt.Bytes(int64(float64(progress.bytes)/tookSec)))
	}
	took := time.Now().Sub(begin)
	tookSec := float64(took) / float64(time.Second)
	fmt.Printf("Consumed %d(%s) for %s at %dmsg(%s)/sec\n",
		totalProgress.count, prettyfmt.Bytes(totalProgress.bytes), took,
		int64(float64(totalProgress.count)/tookSec),
		prettyfmt.Bytes(int64(float64(totalProgress.bytes)/tookSec)))
}

type MessageFetcher struct {
	url       string
	httpClt   http.Client
	messages  chan []byte
	closingCh chan struct{}
	wg        sync.WaitGroup
}

func SpawnMessageFetcher(addr, topic, group string) *MessageFetcher {
	useUnixDomainSocket := false
	var baseURL string
	if strings.HasPrefix(addr, "/") {
		fmt.Printf("Using UDS client for %s\n", addr)
		baseURL = "http://_"
		useUnixDomainSocket = true
	} else {
		fmt.Printf("Using net client for %s\n", addr)
		baseURL = fmt.Sprintf("http://%s", addr)
	}
	url := fmt.Sprintf("%s/topics/%s/messages?group=%s", baseURL, topic, group)

	var httpClt http.Client
	if useUnixDomainSocket {
		dial := func(proto, ignoredAddr string) (net.Conn, error) {
			return net.Dial("unix", addr)
		}
		httpClt.Transport = &http.Transport{Dial: dial}
	}

	mf := &MessageFetcher{
		url:       url,
		httpClt:   httpClt,
		messages:  make(chan []byte),
		closingCh: make(chan struct{}),
	}

	mf.wg.Add(1)
	go func() {
		defer mf.wg.Done()
		for {
			message, err := mf.fetchMessage()
			if err != nil {
				fmt.Printf("Failed to fetch a message: err=(%s)\n", err)
				select {
				case <-mf.closingCh:
					return
				case <-time.After(backOffTimeout):
				}
				continue
			}
			if message != nil {
				mf.messages <- message
			}
			select {
			case <-mf.closingCh:
				return
			default:
			}
		}
	}()

	return mf
}

func (mf *MessageFetcher) Messages() <-chan []byte {
	return mf.messages
}

func (mf *MessageFetcher) Stop() {
	close(mf.closingCh)
	mf.wg.Wait()
	close(mf.messages)
}

func (mf *MessageFetcher) fetchMessage() ([]byte, error) {
	res, err := mf.httpClt.Get(mf.url)
	if err != nil {
		if res != nil && res.Body != nil {
			res.Body.Close()
		}
		return nil, fmt.Errorf("Request failed: err=(%s)", err)
	}
	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read response body: err=(%s)", err)
	}
	res.Body.Close()
	if res.StatusCode == http.StatusRequestTimeout {
		return nil, nil
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Error returned: code=%d, content=%s", res.StatusCode, body)
	}
	return body, nil
}
