package org.opennms.nephron;

import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Simple SSL-enabled Kafka container
 */
public class KafkaSSLContainer extends KafkaContainer {

        private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka");
        private static final String DEFAULT_TAG = "5.4.3";

        public KafkaSSLContainer() {
                this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        }

        public KafkaSSLContainer(final DockerImageName dockerImageName) {
                super(dockerImageName);

                addFileSystemBind(getClass().getResource("/ssl").getFile(), "/etc/kafka/secrets", BindMode.READ_ONLY);

                withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,SSL:SSL,PLAINTEXT:PLAINTEXT");
                withEnv("KAFKA_LISTENERS", "SSL://0.0.0.0:" + KAFKA_PORT + ",BROKER://0.0.0.0:9092");

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
                return String.format("SSL://%s:%s", getHost(), getMappedPort(KAFKA_PORT));
        }
}
