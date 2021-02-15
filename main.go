package main

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"context"
	"log"
	"lambda/config"
	lambdaKafka "lambda/kafka"
)

func HandleRequest(ctx context.Context, snsEvent events.SNSEvent) (string, error) {
	kafkaTopic := os.Getenv("KAFKA_WRITE_TOPIC")

	for _, record := range  snsEvent.Records {
		snsRecord := record.SNS

		msgAttribute, err := json.Marshal(snsRecord.MessageAttributes)
		if err != nil {
			log.Println("Error marshalling the SNS message attributes", err)
			return "",  err
		}

		kafkaHeaders := []kafka.Header{
			kafka.Header{
				Key:   config.MessageAttributes,
				Value: msgAttribute,
			},
		}

		err = lambdaKafka.WriteToKafka([]byte(snsRecord.Message), kafkaTopic, &kafkaHeaders)
		if err != nil {
			log.Println("Error writing to Kafka", err)
			return "",  err
		}
	}

	return "", nil
}

func main() {
	lambda.Start(HandleRequest)
}

