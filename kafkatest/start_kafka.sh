#!/bin/zsh

rm -rf /tmp/kafka-logs
rm -rf /tmp/zookeeper/*

kafkadir="kafkaserver"
kafka_version="2.8.0"
kafka_archive=${kafkadir}/kafka-"${kafka-version}".tgz

if [ ! -f "${kafka_archive}" ]; then
  echo "downloading and installing kafka"
  curl https://www.mirrorservice.org/sites/ftp.apache.org/kafka/2.8.0/kafka_2.13-${kafka_version}.tgz --output "${kafka_archive}"
  tar -xf ${kafka_archive} -C ${kafkadir}
fi

serverdir=${kafkadir}/kafka_2.13-${kafka_version}

${serverdir}/bin/zookeeper-server-start.sh ${serverdir}/config/zookeeper.properties &
echo "waiting for zookeeper to start"
sleep 10
${serverdir}/bin/kafka-server-start.sh ${serverdir}/config/server.properties &
echo "waiting for kafka to start"
sleep 10
echo "script complete"

