/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.kafka;

import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.server.BrokerServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

public class KafkaSourceEmbeddedKafka {

    private static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
    private static final String PLAINTEXT_LISTENER_NAME = "PLAINTEXT";
    private static final String SSL_LISTENER_NAME = "SSL";

    public static final String HOST = InetAddress.getLoopbackAddress().getCanonicalHostName();

    private final KafkaClusterTestKit cluster;
    private final String bootstrapServers;
    private final String bootstrapSslServers;
    private final String bootstrapSslIpPortServers;
    private AdminClient adminClient;
    private KafkaProducer<String, byte[]> producer;

    public KafkaSourceEmbeddedKafka(Properties properties) {
        try {
            File baseDirectory = new File("target/kafka-source-cluster-" + UUID.randomUUID()).getAbsoluteFile();
            TestKitNodes nodes = new TestKitNodes.Builder()
                    .setCombined(true)
                    .setNumControllerNodes(1)
                    .setNumBrokerNodes(1)
                    .setBaseDirectory(baseDirectory.toPath())
                    .setBrokerListenerName(ListenerName.normalised(PLAINTEXT_LISTENER_NAME))
                    .setBrokerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .setControllerListenerName(ListenerName.normalised(CONTROLLER_LISTENER_NAME))
                    .setControllerSecurityProtocol(SecurityProtocol.PLAINTEXT)
                    .build();

            KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes)
                    .setConfigProp(
                            "listeners",
                            PLAINTEXT_LISTENER_NAME + "://localhost:0,"
                                    + SSL_LISTENER_NAME + "://localhost:0,"
                                    + CONTROLLER_LISTENER_NAME + "://localhost:0")
                    .setConfigProp(
                            "listener.security.protocol.map",
                            PLAINTEXT_LISTENER_NAME + ":PLAINTEXT,"
                                    + SSL_LISTENER_NAME + ":SSL,"
                                    + CONTROLLER_LISTENER_NAME + ":PLAINTEXT")
                    .setConfigProp("inter.broker.listener.name", PLAINTEXT_LISTENER_NAME)
                    .setConfigProp("controller.listener.names", CONTROLLER_LISTENER_NAME)
                    .setConfigProp("offsets.topic.replication.factor", "1")
                    .setConfigProp("transaction.state.log.replication.factor", "1")
                    .setConfigProp("transaction.state.log.min.isr", "1")
                    .setConfigProp("transaction.state.log.num.partitions", "1")
                    .setConfigProp("auto.create.topics.enable", "false")
                    .setConfigProp(SSL_TRUSTSTORE_LOCATION_CONFIG, "src/test/resources/truststorefile.jks")
                    .setConfigProp(SSL_TRUSTSTORE_PASSWORD_CONFIG, "password")
                    .setConfigProp(SSL_KEYSTORE_LOCATION_CONFIG, "src/test/resources/keystorefile.jks")
                    .setConfigProp(SSL_KEYSTORE_PASSWORD_CONFIG, "password")
                    .setConfigProp(SSL_KEY_PASSWORD_CONFIG, "password");
            if (properties != null) {
                for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                    builder.setConfigProp(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            cluster = builder.build();
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            bootstrapServers = cluster.bootstrapServers();
            int sslPort = getBoundPort(SSL_LISTENER_NAME);
            bootstrapSslServers = HOST + ":" + sslPort;
            bootstrapSslIpPortServers = "127.0.0.1:" + sslPort;
            initProducer();
        } catch (Exception e) {
            throw new RuntimeException("Unable to start embedded Kafka cluster", e);
        }
    }

    public void stop() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (adminClient != null) {
            adminClient.close();
            adminClient = null;
        }
        try {
            cluster.close();
        } catch (Exception e) {
            throw new IOException("Unable to stop embedded Kafka cluster", e);
        }
    }

    public String getZkConnectString() {
        return null;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getBootstrapSslServers() {
        return bootstrapSslServers;
    }

    public String getBootstrapSslIpPortServers() {
        return bootstrapSslIpPortServers;
    }

    private void initProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1");
        producer = new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer());
    }

    public void produce(String topic, String k, String v) {
        produce(topic, k, v.getBytes());
    }

    public void produce(String topic, String k, byte[] v) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, k, v);
        try {
            producer.send(rec).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while producing message", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error producing message", e);
        }
    }

    public void produce(String topic, int partition, String k, String v) {
        produce(topic, partition, k, v.getBytes());
    }

    public void produce(String topic, int partition, String k, byte[] v) {
        this.produce(topic, partition, null, k, v, null);
    }

    public void produce(String topic, int partition, Long timestamp, String k, byte[] v, Headers headers) {
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, partition, timestamp, k, v, headers);
        try {
            producer.send(rec).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while producing message", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Error producing message", e);
        }
    }

    public void createTopic(String topicName, int numPartitions) {
        AdminClient adminClient = getAdminClient();
        NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
        CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
        Throwable throwable = null;
        for (int i = 0; i < 10; ++i) {
            try {
                result.all().get(1, TimeUnit.SECONDS);
                throwable = null;
                break;
            } catch (Exception e) {
                throwable = e;
            }
        }
        if (throwable != null) {
            throw new RuntimeException("Error getting topic info", throwable);
        }
    }

    private AdminClient getAdminClient() {
        if (adminClient == null) {
            final Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_1");
            adminClient = AdminClient.create(props);
        }
        return adminClient;
    }

    public void deleteTopics(List<String> topic) {
        getAdminClient().deleteTopics(topic);
    }

    private int getBoundPort(String listenerName) {
        BrokerServer broker = cluster.brokers().values().iterator().next();
        return broker.boundPort(ListenerName.normalised(listenerName));
    }
}
