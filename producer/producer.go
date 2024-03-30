package producer

import (
	"encoding/json"
	"log"
	"stream/models"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var BATCH_SIZE = models.BATCH_SIZE

type MessageProducer struct {
	P                   *kafka.Producer
	Topic               *string
	DeliveryCh          chan kafka.Event
	NotifyBatchProduced chan struct{}
	TotalProduced       int
	Mu                  sync.Mutex
}

func NewMessageProducer(topic string, clientId string) *MessageProducer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"client.id":                clientId,
		"enable.idempotence":       true,
		"allow.auto.create.topics": true,
		"acks":                     "all"})
	if err != nil {
		log.Fatalf("Failed to create producer %v", err.Error())
	}
	return &MessageProducer{
		P:                   p,
		Topic:               &topic,
		DeliveryCh:          make(chan kafka.Event, 10000),
		NotifyBatchProduced: make(chan struct{}),
		TotalProduced:       0,
	}
}

func (mp *MessageProducer) Produce(message models.Message) error {
	//marshall message to []byte
	payload, _ := json.Marshal(message)
	err := mp.P.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: mp.Topic, Partition: kafka.PartitionAny},
		Value:          payload},
		mp.DeliveryCh,
	)
	if err != nil {
		log.Printf("Failed to produce message %v\n on topic %s\n%v", message, *mp.Topic, err.Error())
		return err
	}
	mp.Mu.Lock()
	mp.TotalProduced++
	if mp.TotalProduced%BATCH_SIZE == 0 {
		log.Println("True")
		mp.NotifyBatchProduced <- struct{}{}
	}

	log.Printf("TotalProduced : %d \n produced %v", mp.TotalProduced, message)
	mp.Mu.Unlock()
	return nil
}

func (mp *MessageProducer) ProduceBatch(messages []models.Message) {
	//tot := len(messages)
	for _, msg := range messages {
		go mp.Produce(msg)
	}
}
