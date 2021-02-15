package kafka

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	awskafka "github.com/aws/aws-sdk-go/service/kafka"
	"os"
	"log"
)

var p *kafka.Producer
var sess *session.Session

func init() {
	clusterArn := os.Getenv("MSK_CLUSTER_ARN")
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	var err error
	awsRegion := os.Getenv("AWS_REGION")

	svc := awskafka.New(sess, &aws.Config{Region: &awsRegion})
	result, bootstrapErr := svc.GetBootstrapBrokers(&awskafka.GetBootstrapBrokersInput{
		ClusterArn: aws.String(clusterArn),
	})
	if bootstrapErr != nil {
		log.Println("Error fetching broker URL", bootstrapErr)
		return
	}

	broker := *result.BootstrapBrokerStringTls
	p, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                   broker,
		"security.protocol":                   "ssl",
		"enable.idempotence":                  "true",
		"enable.ssl.certificate.verification": false,
	})
	if err != nil {
		log.Println("Error initialising Kafka Producer", err)
		return
	}
}

// WriteToKafka will invoke a Kafka Producer which will write to a Kafka topic
func WriteToKafka(json []byte, topic string, headers *[]kafka.Header) error {
	deliveryChan := make(chan kafka.Event)
	_ = p.Produce(&kafka.Message{
		Headers: *headers,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: json,
	}, deliveryChan)

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		close(deliveryChan)
		log.Println("Delivery failed: Error", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	close(deliveryChan)
	return nil
}

