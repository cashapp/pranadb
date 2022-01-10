#!/bin/zsh

kafka_version="2.8.1"

kafkaserver/kafka_2.13-${kafka_version}/bin/kafka-topics.sh --create --topic payments --partitions 25 --bootstrap-server localhost:9092
