# Nephron [![CircleCI](https://circleci.com/gh/OpenNMS/nephron/tree/master.svg?style=svg)](https://circleci.com/gh/OpenNMS/nephron/tree/master)

Streaming analytics for telemetry & flows.

## Architecture

Sentinel -> Kafka -> Nephron / Beam / Flink -> Elasticsearch

## Run directly

Build & run:
```
mvn package
java -jar target/nephron-bundled-1.0.0-SNAPSHOT.jar
```

## Run on Flink

>  We require Flink 1.9.x since Apache Beam is not yet compatible with Flink 1.10.x.

Build for Flink
```
mvn clean package -Pflink-runner
```

Run on Flink
```
./bin/flink run -c org.opennms.nephron.Nephron /root/git/nephron/target/nephron-bundled-1.0.0-SNAPSHOT.jar --runner=FlinkRunner --jobName=nephron --checkpointingInterval=600000 --autoCommit=false
```

### Upgradine the code

Stopping the job with a savepoint hangs currently, so we need to cancel the job and re-run a new one.

### Metrics

Load Prometheus exporter on Flink:
```
cp opt/flink-metrics-prometheus-1.9.2.jar lib/
cp opt/flink-metrics-slf4j-1.9.2.jar lib/
```

Append to `conf/flink-conf.yaml`:
```
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9250-9260
metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter
metrics.reporter.slf4j.interval: 60 SECONDS
```

TODO: Finish Prometheus collector and write DC definitions for it -> good use case

#### Using savepoints

Here's what the procedure *should* be when applying patches.

Rebuild the artifact in place.

Find the job id:
```
./bin/flink list
```

Stop the job with a savepoint:
```
./bin/flink stop 99d87bc4d3a271a72f3f89dfe5a904d7 -p /tmp/state
```

> TODO: This hangs.

Now restart the job with:
```
???
```

## Elasticsearch

Install the template using:
```
curl -XPUT -H 'Content-Type: application/json' http://localhost:9200/_template/netflow_agg -d@./src/main/resources/aggregated-flows-template.json
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

# References

* Streaming 101 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
* Streaming 102 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/

