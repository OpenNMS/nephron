# Nephron [![CircleCI](https://circleci.com/gh/OpenNMS/nephron/tree/master.svg?style=svg)](https://circleci.com/gh/OpenNMS/nephron/tree/master)

Streaming analytics for telemetry & flows.

## Architecture

Sentinel -> Kafka -> Nephron / Beam / Spark -> Elasticsearch

## Setup

Build & run:
```
mvn package
java -jar target/nephron-1.0.0-SNAPSHOT-jar-with-dependencies.jar
```

### OpenNMS Configuration

On OpenNMS or Sentinel, enable the Kafka exporter for flows:
```
config:edit org.opennms.features.flows.persistence.kafka
config:property-set topic opennms-flows
config:property-set bootstrap.servers 127.0.0.1:9092
config:update

config:edit org.opennms.features.flows.persistence.elastic
config:property-set enableForwarding true
config:update
```

and enable the Kafka exporter for telemetry:
```
config:edit org.opennms.features.kafka.producer.client
config:property-set bootstrap.servers 127.0.0.1:9092
config:update

config:edit org.opennms.features.kafka.producer
config:property-set eventTopic ""
config:property-set alarmTopic ""
config:property-set nodeTopic "opennms-nodes"
config:property-set metricTopic "opennms-metrics"
config:property-set forward.metrics true
config:update

feature:install opennms-kafka-producer
```

## Telemetry

* Top K interfaces system wide
* Top K interfaces per location
* Top K interfaces per category
* Top K interfaces per type

* System with highest load average
...

## Flows

* Top K applications system wide
* Top K applications per device
...

## Other workloads to consider

* [events] Top K event types
* [events] Top K devices sending events
* [alarms] Top K affected components by alarms
* [bmp] Top K updates by prefix
