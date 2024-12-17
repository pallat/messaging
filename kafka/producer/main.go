package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"log"

	_ "net/http/pprof"

	"github.com/IBM/sarama"
)

func main() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092","localhost:9093","localhost:9094"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	var enqueued, producerErrors int
	for range 5 {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "demo", Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueued++
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}