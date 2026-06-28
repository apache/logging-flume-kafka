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
package org.apache.flume.sink.kafka.util;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import kafka.server.BrokerServer;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

public class KafkaLocal {

    private static final String CONTROLLER_LISTENER_NAME = "CONTROLLER";
    private static final String PLAINTEXT_LISTENER_NAME = "PLAINTEXT";
    private static final String SSL_LISTENER_NAME = "SSL";

    private final KafkaClusterTestKit kafka;
    private String bootstrapServers;
    private String bootstrapSslServers;

    public KafkaLocal(Properties kafkaProperties) throws Exception {
        File baseDirectory = new File("target/kafka-sink-cluster-" + UUID.randomUUID()).getAbsoluteFile();
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
                .setConfigProp("auto.create.topics.enable", "false");
        if (kafkaProperties != null) {
            for (Map.Entry<Object, Object> entry : kafkaProperties.entrySet()) {
                builder.setConfigProp(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        kafka = builder.build();
    }

    public void start() throws Exception {
        kafka.format();
        kafka.startup();
        kafka.waitForReadyBrokers();
        bootstrapServers = kafka.bootstrapServers();
        bootstrapSslServers = "localhost:" + getBoundPort(SSL_LISTENER_NAME);
    }

    public void stop() {
        try {
            kafka.close();
        } catch (Exception e) {
            throw new RuntimeException("Error stopping embedded Kafka cluster", e);
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getBootstrapSslServers() {
        return bootstrapSslServers;
    }

    private int getBoundPort(String listenerName) {
        BrokerServer broker = kafka.brokers().values().iterator().next();
        return broker.boundPort(ListenerName.normalised(listenerName));
    }
}
