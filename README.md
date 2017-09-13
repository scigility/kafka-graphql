# GraphKL framework - GraphQL Kafka Language

Powerfull and simple streaming framework into a intuitive Graphql enviroment for Apache Kafka out-of-the-box applications. 

GraphKL goes beyond GraphQl and beyond Apache Kafka, providing simple solution to read, write, and process streaming data in real-time, at scale, using the powerfull GraphQL-like semantics, with dynamic query, mutations, subcriptions, and fragmented queries.

It offers an easy way to express stream processing transformations as an alternative to writing an application in a programming language such as Java or Python.

Currently available as a developer preview, GraphKL provides powerful stream processing capabilities such as joins, aggregations, event-time windowing, and more!

## Quickstart

Download the repo:
$ git clone https://github.com/scigility/kafka-graphql.git

Edit the docker-compose.yml file to set your ip into kafka advertiser properties in the Docker:
KAFKA_ADVERTISED_HOST_NAME
KAFKA_ADVERTISED_LISTENERS

Then run the kafka and zookeeper:
docker-compose up

After this, build the application:
mvn clean package

Now you a ready to run the application:
java -jar target/graphql-spring-boot-starter-sample-1.0.3-alpha.jar


You have access to the graphql interface via http://localhost:8080/v1/graphql

mutation updateKafkaMutation {
  updateKafka(input: {
		broker: "192.168.171.135:9092"
		zookeeper: "192.168.171.135:2181"
	}) {broker,zookeeper topics{name}}
}

query {
	 kafka{broker,zookeeper, topics{name}}
}

mutation addKafkaTopicMutation {
  addKafkaTopic(input: {
		topic_name: "test"
	})

	{topics{name}}
}

query {
	topics{
		name
	}
}

mutation produceRecord {
  produceTopicRecord(input: {name: "test", message: "howdy graphql"}) {
    key
    value
    offset
    partition
  }
}


mutation consumeRecord {
  consumeTopicRecord(input: {name: "test"}) {
    key
    value
    offset
    partition
  }
}

mutation startStream {
  streamStart(input: {input:"in" output:"out"}) {
		key
		value
  }
}

query {
	stream{
		key value
	}
}

## Ask for what you need, get exactly that

## Get many resources in a single request

## Move faster with powerful developer tools

## Evolve your API without versions

## Bring your own data and code

## Who’s using GraphKL?

## Get Started

If you are ready to see the power of GraphKL, try out these:

GraphKL Quick Start: Demonstrates a simple workflow using GraphKL to write streaming queries against data in Kafka.
Clickstream Analysis Demo: Shows how to build an application that performs real-time user analytics.

## Learn More

You can find the GraphKL documentation here.

## Join the Community

Whether you need help, want to contribute, or are just looking for the latest news, you can find out how to connect with your fellow Confluent community members here.

Ask a question in the #GraphKL channel in our public Confluent Community Slack. Account registration is free and self-service.
Join the Confluent Google group.

## Contributing

Contributions to the code, examples, documentation, etc, are very much appreciated. For more information, see the contribution guidelines.

Report issues and bugs directly in this GitHub project.

##License

The project is licensed under the Apache License, version 2.0.

Apache, Apache Kafka, Kafka, and associated open source project names are trademarks of the Apache Software Foundation.