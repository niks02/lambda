# Lambda
- Lambda will get triggered on an SNS event and will write to specified Kafka topic

# Env Variables
- MSK_CLUSTER_ARN - Kafka ARN to fetch broker URL
- AWS_REGION - AWS region where lambda is present
- KAFKA_WRITE_TOPIC - Kafka topic where lambda will write

# Building the service
- After cloning the repo, go to the directory where repo is cloned
- Set the environment variables and execute the following commands
- "go build ."
- "./lambda"

# Building Docker Image
- docker build -t lambda .
