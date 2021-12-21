0.3.1
=====

This release contains an update to the latest Log4j2 (2.17.0).

* bump log4j version (Issue [NMS-13871](https://issues.opennms.org/browse/NMS-13871))

0.3.0
=====

This release contains a number of updates including performance improvements, an
update to Java 11 and Flink 1.13, and adds support for output to Cortex.

Note that this release removes the redundant or unnecessary `windowStart`,
`windowEnd`, `ranking`, and `group_by_key` fields.

* RandomFlowIT flaps in CircleCI (Issue [NMS-13282](https://issues.opennms.org/browse/NMS-13282))
* Replace FlowTimestampPolicy by shipped CustomTimestampPolicyWithLimitedDelay (Issue [NMS-13310](https://issues.opennms.org/browse/NMS-13310))
* Reduce Memory Churn (Issue [NMS-13326](https://issues.opennms.org/browse/NMS-13326))
* Provide the ability in Nephron to write to Cortex (Issue [NMS-13373](https://issues.opennms.org/browse/NMS-13373))
* CortexIo GC global state (Issue [NMS-13453](https://issues.opennms.org/browse/NMS-13453))
* add more tests (Issue [NMS-13466](https://issues.opennms.org/browse/NMS-13466))
* Test if pane accumulation has a positive effect on performance (Issue [NMS-13467](https://issues.opennms.org/browse/NMS-13467))
* Support running on Flink 1.13 (Issue [NMS-13535](https://issues.opennms.org/browse/NMS-13535))
* Remove unnecessary fields from FlowSummary / FlowSummaryData (Issue [NMS-13545](https://issues.opennms.org/browse/NMS-13545))
* FlowAnalyzerIT flaps on Flink 1.13 (Issue [NMS-13546](https://issues.opennms.org/browse/NMS-13546))
* RandomFlowIT.testRatesWithClockSkew is still flaky (Issue [NMS-13571](https://issues.opennms.org/browse/NMS-13571))
* Remove FlowSummaryData (Issue [NMS-13578](https://issues.opennms.org/browse/NMS-13578))

0.2.1
=====

This was a re-release of 0.2.0 to fix some issues with deploying to Maven Central.

0.2.0
=====

This release contains a ton of algorithmic and bug improvements, as well as the
addition of DSCP ToS Quality of Service support.

* the default Elasticsearch index strategy is now monthly instead of daily
* node ID is now included in the export key when foreignSource/foreignId is present
* hostnames are no longer part of keys ([NMS-12692](https://issues.opennms.org/browse/NMS-12692))
* Nephron options now includes configuration for connection and socket timeouts, as well as retries
* DNS aggregation performance has been improved ([NMS-12975](https://issues.opennms.org/browse/NMS-12975))
* A number of bugs in flow aggregation, skewed clocks, and bucket handling have been fixed ([NMS-12967](https://issues.opennms.org/browse/NMS-12967),
  [NMS-13099](https://issues.opennms.org/browse/NMS-13099))
* A number of algorithmic improvements for aggregation have been made ([NMS-13115](https://issues.opennms.org/browse/NMS-13115),
  [NMS-13116](https://issues.opennms.org/browse/NMS-13116), [NMS-13198](https://issues.opennms.org/browse/NMS-13198),
  [NMS-13100](https://issues.opennms.org/browse/NMS-13100))
* Support has been added for aggregating DSCP ToS Quality of Service ([NMS-12945](https://issues.opennms.org/browse/NMS-12945))
* SSL is now supported ([NMS-13203](https://issues.opennms.org/browse/NMS-13203))

0.1.1
=====

Initial release.
