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
package org.apache.flume.shared.kafka.test;

import java.util.Locale;
import org.junit.Assume;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Skips the tests that run an embedded Kafka broker on Windows.
 * <p>
 * Kafka does not support brokers on Windows: deleting a topic renames and deletes memory-mapped segment files,
 * which Windows refuses, so the broker marks its only log directory as failed and halts the JVM
 * (see <a href="https://issues.apache.org/jira/browse/KAFKA-1194">KAFKA-1194</a>).
 * </p>
 * <p>
 * Use it as a {@code @ClassRule}, so that neither {@code @BeforeClass} nor {@code @AfterClass} runs on Windows.
 * </p>
 */
public final class SkipOnWindows implements TestRule {

    private static final boolean IS_WINDOWS =
            System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Assume.assumeFalse("Kafka brokers are not supported on Windows", IS_WINDOWS);
                base.evaluate();
            }
        };
    }
}
