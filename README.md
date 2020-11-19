# Nephron [![CircleCI](https://circleci.com/gh/OpenNMS/nephron/tree/master.svg?style=svg)](https://circleci.com/gh/OpenNMS/nephron/tree/master)

Streaming analytics for flows.

## Architecture

Sentinel -> Kafka -> Nephron / Beam / Flink -> Elasticsearch

## Building

Build & run:
```
mvn clean package
```

## Run on Flink

>  We require Flink 1.10.x 

Run on Flink
```
./bin/flink run --parallelism 1 --class org.opennms.nephron.Nephron /root/git/nephron/assemblies/flink/target/nephron-flink-bundled-*.jar --runner=FlinkRunner --jobName=nephron --checkpointingInterval=600000 --autoCommit=false
```

### Upgrading the code

To restart the job and keep the current state, see the instructions belllow.

To restart the job without keeping the state, cancel the job and re-run a new one.

### Metrics

#### Prometheus

Load Prometheus exporter on Flink:
```
cp opt/flink-metrics-prometheus-1.9.2.jar lib/
```

Append to `conf/flink-conf.yaml`:
```
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.port: 9250-9260
```
#### SLF4J

Load the SLF4J report on Flink:
```
cp opt/flink-metrics-slf4j-1.9.2.jar lib/
```

Append to `conf/flink-conf.yaml`:
```
metrics.reporter.slf4j.class: org.apache.flink.metrics.slf4j.Slf4jReporter
metrics.reporter.slf4j.interval: 60 SECONDS
```

### Restart job w/ savepoints

Rebuild the artifact in place.

Find the job id:
```
./bin/flink list
```

Stop the job with a savepoint:
```
./bin/flink stop 99d87bc4d3a271a72f3f89dfe5a904d7 -p /tmp/state
```

Now restart the job with:
```
./bin/flink run --parallelism 1 --fromSavepoint /tmp/state/savepoint-5cfdc5-00788010255a --class org.opennms.nephron.Nephron /root/git/nephron/assemblies/flink/target/nephron-flink-bundled-*.jar --runner=FlinkRunner --jobName=nephron --checkpointingInterval=600000 --autoCommit=false
```

## Elasticsearch

Install the template using:
```
curl -XPUT -H 'Content-Type: application/json' http://localhost:9200/_template/netflow_agg -d@./main/src/main/resources/netflow_agg-template.json
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

# References

* Streaming 101 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
* Streaming 102 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/

