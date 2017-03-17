package service

import (
	"testing"

	"github.com/Shopify/sarama"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

func assertMsgs(c *C, consumed map[string][]*pb.ConsRs, produced map[string][]*sarama.ProducerMessage) {
	for key, prodMsgs := range produced {
		for i, prodMsg := range prodMsgs {
			consRess, ok := consumed[key]
			if !ok {
				c.Errorf("key `%s` has not been consumed", key)
				return
			}
			if i >= len(consRess) {
				c.Errorf("too few consumed: key=%s, got=%d", key, len(consRess))
				return
			}
			consRes := consumed[key][i]
			if consRes.Offset != prodMsg.Offset ||
				consRes.Partition != prodMsg.Partition ||
				string(consRes.KeyValue) != string(prodMsg.Key.(sarama.StringEncoder)) ||
				string(consRes.Message) != string(prodMsg.Value.(sarama.StringEncoder)) {
				c.Errorf("want %+v, got %+v", prodMsg, consRes)
			}
		}
	}
}
