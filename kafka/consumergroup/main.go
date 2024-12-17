package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}

	group, err := sarama.NewConsumerGroup([]string{"localhost:9092","localhost:9093","localhost:9094"}, "arise", config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := group.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	ctx := context.Background()
	for {
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, []string{"demo"}, handler)
		if err != nil {
			panic(err)
		}
	}
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
consume:
	for {
		select {
		case msg := <-claim.Messages():
			fmt.Printf("Message topic:%q partition:%d offset:%d message:%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
			sess.MarkMessage(msg, "")
		case <-sess.Context().Done():
			fmt.Println("rebalance")
			break consume
		}
	}
	return sess.Context().Err()
}