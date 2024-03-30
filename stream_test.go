package main

import (
	"fmt"
	"log"
	"stream/consumer"
	"stream/database"
	"stream/models"
	"stream/producer"
	"testing"
	"time"
)

var (
	BATCH_SIZE   = models.BATCH_SIZE
	totalBatches = 1
	msgId        = 0
	test_id      = fmt.Sprintf("%d", time.Now().Unix())
)

func ProduceBatchThread(p *producer.MessageProducer) {
	//produce batch logic
	for batch := 0; batch < totalBatches; batch++ {
		var msgs []models.Message
		for size := 0; size < BATCH_SIZE; size++ {
			data := fmt.Sprintf("Message number %d of batch %d", msgId, batch)
			var status string
			if size%2 == 0 {
				status = "complete"
			} else if size%3 == 0 {
				status = "failed"
			} else {
				status = "progress"
			}
			msg := models.Message{
				Id:           uint64(msgId),
				Data:         data,
				Status:       status,
				KafkaTopicId: test_id,
				Batch:        uint64(batch),
			}
			msg.Checksum = models.GenerateCheckSum(msg)
			msgs = append(msgs, msg)
			msgId++
		}
		//produce batch in a separate thread mimicking server
		go p.ProduceBatch(msgs)
	}

}

func ConsumeThread(c *consumer.MessageConsumer) {
	go c.Consume()
}
func Test(t *testing.T) {
	//make producer
	var (
		topic  = "test" + test_id
		client = "foo" + test_id
		group  = "bar" + test_id
	)
	p := producer.NewMessageProducer(topic, client)

	go ProduceBatchThread(p)

	time.Sleep(2 * time.Second)
	c := consumer.NewMessageConsumer(group)
	c.Subscribe(topic)

	db := database.NewDb()
	go ConsumeThread(c)

	var (
		totalBatchedProduced = 0
		totalBatchesConsumed = 0
	)

	//when a batch is produced it notifies via BatchProduceNotify channel
	//likewise when a batch is consumed it notifies via BatchConsumeNotify channel
	for p.NotifyBatchProduced != nil || c.NotifyBatchConsumed != nil {
		select {
		case _, ok := <-p.NotifyBatchProduced:
			if !ok {
				//channel closed
				log.Printf("Produced Shut Down")
				p.NotifyBatchProduced = nil
				continue
			}
			log.Printf("Batch Produced by Producer")
			totalBatchedProduced++
			if totalBatchedProduced == totalBatches {
				//all batches in testing produced
				close(p.NotifyBatchProduced)
			}
		case _, ok := <-c.NotifyBatchConsumed:
			if !ok {
				//channel closed
				log.Printf("Consumer Shut Down")
				c.NotifyBatchConsumed = nil
				continue
			}
			log.Printf("Batch Consumed by Consumer")
			totalBatchesConsumed++
			if totalBatchesConsumed == totalBatches {
				//all batches in testing consumed
				close(c.NotifyBatchConsumed)
			}
		}
	}

	//now both the producer and consumer are done
	//test_ok := true
	var msgs []models.Message
	var tot int64
	db.Find(&msgs).Where("kafka_topic_id", test_id).Count(&tot)

	log.Printf("Total Messages in DB : %d \n", tot)
	log.Println("P : ", totalBatchedProduced)
	log.Println("C : ", totalBatchesConsumed)

	totalMsgsProduced := totalBatches * BATCH_SIZE
	//Count Check
	if tot != int64(totalMsgsProduced) {
		t.Errorf("Total Messages Produced %d \n Recorded in DB %d", totalMsgsProduced, tot)
	} else {
		t.Logf("Count Test Pass")
	}

	//message integrity test for status and other data
	totInvalid := 0
	for _, msg := range msgs {
		isValid := models.CheckSum(msg)
		if !isValid {
			totInvalid++
		}
	}
	if totInvalid > 0 {
		t.Errorf("%d Number of Messages in Database found corrupt", totInvalid)
	} else {
		t.Logf("Integrity Test Pass")
	}
}
