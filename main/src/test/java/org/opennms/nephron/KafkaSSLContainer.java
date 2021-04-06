package org.opennms.nephron;

import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectContainerResponse;

/**
 * Simple SSL-enabled Kafka container
 */
public class KafkaSSLContainer extends GenericContainer<KafkaSSLContainer> {

        private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");
        private static final String DEFAULT_TAG = "5.4.3";

        private static final String STARTER_SCRIPT = "/testcontainers_start.sh";

        public static final int KAFKA_PORT = 9093;

        public static final int ZOOKEEPER_PORT = 2181;

        private static final int PORT_NOT_ASSIGNED = -1;

        private int port = PORT_NOT_ASSIGNED;

        public KafkaSSLContainer() {
                this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        }

        public KafkaSSLContainer(final DockerImageName dockerImageName) {
                super(dockerImageName);
                addFileSystemBind(getClass().getResource("/ssl").getFile(), "/etc/kafka/secrets", BindMode.READ_ONLY);
                dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
                withExposedPorts(KAFKA_PORT);

                withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,SSL:SSL,PLAINTEXT:PLAINTEXT");
                withEnv("KAFKA_LISTENERS", "SSL://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092");
                withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

                withEnv("KAFKA_BROKER_ID", "1");
                withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
                withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
                withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
                withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");

                withEnv("KAFKA_SSL_KEYSTORE_FILENAME","server.keystore");
                withEnv("KAFKA_SSL_KEYSTORE_CREDENTIALS","secret");
                withEnv("KAFKA_SSL_KEY_CREDENTIALS","secret");
                withEnv("KAFKA_SSL_TRUSTSTORE_FILENAME","server.truststore");
                withEnv("KAFKA_SSL_TRUSTSTORE_CREDENTIALS","secret");
                withEnv("KAFKA_SSL_CLIENT_AUTH", "required");
                withEnv("KAFKA_ADVERTISED_HOST_NAME","localhost");
                withEnv("KAFKA_SECURITY_PROTOCOL", "SSL");
        }

        public String getBootstrapServers() {
                if (port == PORT_NOT_ASSIGNED) {
                        throw new IllegalStateException("You should start Kafka container first");
                }
                return String.format("SSL://%s:%s", getHost(), port);
        }

        @Override
        protected void doStart() {
                withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
                addExposedPort(ZOOKEEPER_PORT);
                super.doStart();
        }

        @Override
        protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
                super.containerIsStarting(containerInfo, reused);

                port = getMappedPort(KAFKA_PORT);

                if (reused) {
                        return;
                }

                String command = "#!/bin/bash\n";
                command += "echo 'clientPort=" + ZOOKEEPER_PORT + "' > zookeeper.properties\n";
                command += "echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties\n";
                command += "echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties\n";
                command += "zookeeper-server-start zookeeper.properties &\n";

                command += "export KAFKA_ZOOKEEPER_CONNECT='localhost:" + ZOOKEEPER_PORT + "'\n";
                command += "export KAFKA_ADVERTISED_LISTENERS='" + Stream
                        .concat(
                                Stream.of(getBootstrapServers()),
                                containerInfo.getNetworkSettings().getNetworks().values().stream()
                                        .map(it -> "BROKER://" + it.getIpAddress() + ":9092")
                        )
                        .collect(Collectors.joining(",")) + "'\n";

                command += ". /etc/confluent/docker/bash-config \n";
                command += "/etc/confluent/docker/configure \n";
                command += "/etc/confluent/docker/launch \n";

                copyFileToContainer(
                        Transferable.of(command.getBytes(StandardCharsets.UTF_8), 0777),
                        STARTER_SCRIPT
                );
        }
}
