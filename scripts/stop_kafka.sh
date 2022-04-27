#!/bin/zsh

kafkadir="kafkaserver"
kafka_version="2.8.1"
serverdir=${kafkadir}/kafka_2.13-${kafka_version}

${serverdir}/bin/kafka-server-stop.sh
${serverdir}/bin/zookeeper-server-stop.sh

rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper/*

