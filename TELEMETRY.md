# Telemtry

## OpenNMS Configuration

Enable the Kafka exporter for telemetry:
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

## Other workloads to consider

* [events] Top K event types
* [events] Top K devices sending events
* [alarms] Top K affected components by alarms
* [bmp] Top K updates by prefix

# References

* Streaming 101 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-101/
* Streaming 102 - https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/

