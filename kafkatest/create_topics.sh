#!/bin/zsh

kafka_version="2.8.0"

kafkaserver/kafka_2.13-${kafka_version}/bin/kafka-topics.sh --create --topic testtopic --partitions 25 --bootstrap-server localhost:9092
