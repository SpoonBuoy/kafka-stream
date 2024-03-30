package models

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

var BATCH_SIZE = 10

type Message struct {
	Pkey         uint64 `gorm:"primary_key"`
	Id           uint64
	Data         string
	Status       string
	Checksum     string
	KafkaTopicId string
	Batch        uint64
}

func GenerateCheckSum(msg Message) string {
	concatenated := strconv.FormatUint(msg.Id, 10) + msg.Data + msg.Status + strconv.FormatUint(msg.Batch, 10)
	hash := sha256.Sum256([]byte(concatenated))
	return hex.EncodeToString(hash[:])

}

func CheckSum(msg Message) bool {
	claim := msg.Checksum
	actual := GenerateCheckSum(msg)

	return claim == actual
}
