### Kafka Message Streaming

### Requirements

Before you start using or developing this project, make sure you meet the following requirements:

- **Clone**: 
  ```
  git clone 
  cd msg-stream
  ```
  

- **Kafka Broker**: A Kafka broker running in Docker. This can be installed and started with Docker Compose using the command:
  ```
  docker-compose up -d
  ```
  Ensure you have `docker` and `docker-compose` installed on your system.

- **Go Version**: This project requires Go version 1.2 or higher. You can check your Go version with:
  ```
  go version
  ```
  If you need to install or upgrade Go, follow the instructions on the [official Go website](https://golang.org/dl/).

  NOTE : No need to setup database as the postgres test database is already setup in cloud for this project

## Installation

After ensuring the requirements are met, you can install the project dependencies with:
```
go mod tidy
```

## Running Tests

To run the tests and verify everything is set up correctly, use the following command:
```
go test ./... -v
```

## Explainer
 NOTE : Producer and Consumer run on two separate threads concurrently mimicking the real time producer and consumer servers

 Some Variables : 
 | Variable | Defined In |  Meaning|
|----------|----------|----------|
| BATCH_SIZE | models/Message.go | Defines the batch size |
| totalBatches| stream_test.go | Total batches produced after which  producer notifies the main thread to consider only these amount of batches for testing |
| test_id | stream_test.go | Each new test is given a new test_id to uniquely identify the messages generated in that test |


## How Tests work
**Count Test** <br>
   - Total Messages produced can be known by `totalBatches * batchSize` on the producer side
   - Once the consumer consumes it also keeps the count `totalConsumed` and once `totalConsumed == totalProduced` it informs the main thread that its job is done
   - While consuming a message is sent to `processor` which stores the messages in database. Since the database write operation is blocking it may take some time.
   - In the main thread, once Consumer signals job done, the messages are read from the Database with `kafka_topic_id = test_id` to read messages generated in this test only.
   - The count of these messages is compared to totalMessages produced
   <br>

**Integrity Test** <br>
  - When a message is produced a `Checksum` is calculated and stored in message itself. If any data is mutated in message its new checksum will be different.
   - When message is read from database, its `Checksum` is generated again and compared with its inherent one. If both are found equal then no data corruption has happened, otherwise the message is corrupted.
   - Integrity could further be verified by keeping the count of `progress` messages, `completed` messages, `failed` messages and then comparing with their count in database but its redundant once checksums are used for integrity.