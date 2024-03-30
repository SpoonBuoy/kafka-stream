package consumer

import (
	"encoding/json"
	"log"
	"stream/database"
	"stream/models"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var BATCH_SIZE = models.BATCH_SIZE

type MessageConsumer struct {
	Consumer            *kafka.Consumer
	Topic               string
	NotifyBatchConsumed chan struct{}
	TotalConsumed       int
	Mu                  sync.Mutex
}

func NewMessageConsumer(group string) *MessageConsumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          group,

		"allow.auto.create.topics": true,
		"auto.offset.reset":        "smallest"})
	if err != nil {
		log.Fatalf("failed to create consumer for group %v", group)
	}
	return &MessageConsumer{
		Consumer:            consumer,
		NotifyBatchConsumed: make(chan struct{}),
		TotalConsumed:       0,
	}
}

func (mc *MessageConsumer) Subscribe(topic string) {
	err := mc.Consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("consumer failed to subscribe to topic %s %v", topic, err.Error())
		return
	}
	log.Printf("consumer subscribed to topic %s", topic)
}

func (mc *MessageConsumer) Consume() {

free:
	for {
		ev := mc.Consumer.Poll(100)
		list, _ := mc.Consumer.Assignment()
		pos, _ := mc.Consumer.Position(list)

		switch e := ev.(type) {
		case *kafka.Message:
			log.Println("Consuming")

			log.Printf("Topic %s Partition %d Offset %d : ", *pos[0].Topic, pos[0].Partition, pos[0].Offset)
			var message models.Message
			if err := json.Unmarshal(e.Value, &message); err != nil {
				log.Printf("Failed to Unmarshall %v", e.Value)
			}

			mc.Process(&message)
			mc.Mu.Lock()
			mc.TotalConsumed++
			if mc.TotalConsumed%BATCH_SIZE == 0 {
				mc.NotifyBatchConsumed <- struct{}{}
			}
			mc.Mu.Unlock()
		case kafka.Error:
			log.Printf("%% Error: %v\n", e)
			break free
		default:
			//log.Printf("TotalConsumed Consumed %v\n", mc.TotalConsumed)
		}
	}
}

func (mc *MessageConsumer) Close() error {
	err := mc.Consumer.Close()

	if err != nil {
		log.Printf("Failed to close consumer %v", err.Error())
		return err
	}

	return nil
}

func (mc *MessageConsumer) Process(msg *models.Message) {
	log.Printf("Message : %v", msg)
	//validate message
	log.Println("Integrity Check: ", models.CheckSum(*msg))
	database.DB.Create(&msg)
}
