## Testing support for Nephron

Synthetic flows are generated using a pseudo-random generator. Generation is completely deterministic if the so-called "playback" mode is used. In this case timestamps are calculated based on a given start timestamp. In "non-playback" mode timestamps are based on the current time. The "non-playback" is used to generate current flow data.

Flow generation can be parameterized to a large extend. The configuration options are documented in the `FlowGenOptions` interface.

Streams of synthetic flows can be generated. The corresponding functionality can be found in the `FlowDocuments` class. Alternatively, synthetic flows can be directly fed into processing pipelines by using the `SyntheticFlowSource` class. Finally, they can be ingested into a Kafka topic using the `KafkaFlowIngester` utility class.

The `Benchmark` application class allows to run the Nephron pipeline on synthetic flows. Flows can either be directly fed into the pipeline or via Kafka.
