# Persistence

Nephron aggregates flows over time and by several groups of aspects (i.e. exporters, interfaces, dscps (differentiated service code points), applications, hosts, and conversations).  

Note: `application` and `conversation` are classifications of flows. OpenNMS classifies flows into applications based on rules that consider the protocol, src/dst address, and src/dst port of flows. Flows are classified into the same conversation if they have matching hosts, applications, protocols, and (exporter) locations. (The src/dst host distinction is of no importance for conversation classification.)

Nephron can output flow summary aggregations to different systems:

1. ElasticSearch
1. Kafka
1. Cortex

## ElasticSearch

...

## Cortex

The following metrics are stored:

| metric | complete/topK | labels |
| --- | --- | --- |
| `EXPORTER_INTERFACE` | complete | `pane`, `direction`, `nodeId`, `ifIndex` |
| `EXPORTER_INTERFACE_TOS` | complete | `pane`, `direction`, `nodeId`, `ifIndex`, `dscp` |
| `BY_APP_${nodeId}_${ifIndex}` | topK | `pane`, `direction`, `application` |
| `BY_HOST_${nodeId}_${ifIndex}` | topK | `pane`, `direction`, `host` |
| `BY_TOS_AND_APP_${nodeId}_${ifIndex}` | topK | `pane`, `direction`, `dscp`, `application` |
| `BY_TOS_AND_HOST_${nodeId}_${ifIndex}` | topK | `pane`, `direction`, `dscp`, `host` |

The `nodeId` and `ifIndex` is encoded in metric names for some of the aggregations in order to reduce the product of label cardinalities.

The labels are:

| name | description / remark |
| --- | --- |
| `pane` | additional label to distinguish samples for the same timestamp; used for early / late results and Cortex's time ordering constraint |
| `direction` | possible values are `in` and `out` |
| `nodeId` | the node id of the flow exporter |
| `ifIndex` | interface index |
| `dscp` | possible values: 0-63; differentiated service code points |
| `host`, `host2` | ip addresses in dot notation |
| `application` | the result of classifying flows into applications |
| `protocol` | possible values: integer range; protocol number |
| `location` | TODO: from exporter node meta data? |
