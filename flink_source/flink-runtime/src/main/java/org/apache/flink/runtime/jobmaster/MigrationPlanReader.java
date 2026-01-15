/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for reading migration plans from JSON files.
 * The migration plan maps subtask IDs to target IP addresses.
 * 
 * <p>Expected JSON format:
 * <pre>
 * {
 *   "operator-name_0": "192.168.1.100",
 *   "operator-name_1": "192.168.1.101",
 *   ...
 * }
 * </pre>
 */
public class MigrationPlanReader {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationPlanReader.class);

    // 新增一個 plan 資料夾，專門放 migration_plan ，不要跟source混在一起 不知道這樣設定路徑是否讀取的到？
    private static final String DEFAULT_MIGRATION_PLAN_PATH = 
            "/opt/flink/plan/migration_plan.json";

    private final ObjectMapper objectMapper;

    public MigrationPlanReader() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Reads the migration plan from the default location.
     *
     * @return A map from subtask ID (as string) to target IP address
     */
    public Map<String, String> readMigrationPlan() {
        return readMigrationPlan(DEFAULT_MIGRATION_PLAN_PATH);
    }

    /**
     * Reads the migration plan from the specified file path.
     *
     * @param filePath The path to the migration plan JSON file
     * @return A map from subtask ID (as string) to target IP address
     */
    public Map<String, String> readMigrationPlan(String filePath) {
        Map<String, String> migrationPlan = new HashMap<>();

        File file = new File(filePath);
        if (!file.exists()) {
            LOG.warn("Migration plan file not found at: {}. No IP-based slot allocation will be performed.", filePath);
            return migrationPlan;
        }

        try {
            JsonNode rootNode = objectMapper.readTree(file);

            if (rootNode.isObject()) {
                LOG.info("Parsing migration plan entries...");
                rootNode.fields().forEachRemaining(entry -> {
                    String subtaskId = entry.getKey();
                    JsonNode valueNode = entry.getValue();

                    if (valueNode.isTextual()) {
                        String targetIp = valueNode.asText();
                        migrationPlan.put(subtaskId, targetIp);
                        LOG.info("MigrationPlanReader: Subtask [{}] assigned to Target IP [{}]", 
                                subtaskId, targetIp);
                    } else {
                        LOG.warn("Invalid value format for subtask {}: expected string, got {}", 
                                subtaskId, valueNode.getNodeType());
                    }
                });
            } else {
                LOG.warn("Migration plan file has invalid format: expected JSON object at root");
            }

            LOG.info("Successfully loaded migration plan with {} entries from {}", 
                    migrationPlan.size(), filePath);

        } catch (IOException e) {
            LOG.error("Failed to read migration plan from {}", filePath, e);
        }

        return migrationPlan;
    }

    /**
     * Gets the target IP for a specific subtask.
     *
     * @param subtaskId The subtask ID (e.g., "operator-name_0", "operator-name_1")
     * @param migrationPlan The migration plan map
     * @return Optional containing the target IP if found, empty otherwise
     */
    public static Optional<String> getTargetIp(String subtaskId, Map<String, String> migrationPlan) {
        return Optional.ofNullable(migrationPlan.get(subtaskId));
    }
}
