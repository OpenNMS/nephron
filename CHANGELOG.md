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
