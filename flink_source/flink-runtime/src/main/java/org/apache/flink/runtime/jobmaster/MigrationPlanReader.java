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
 *
 * <p>Supports hybrid mapping strategy with two priority levels:
 * <ol>
 *   <li><b>Priority 1 (Fine-grained):</b> Subtask-level mapping
 *       <br>Maps individual subtasks to target TaskManager resource IDs</li>
 *   <li><b>Priority 2 (Coarse-grained):</b> Slot-sharing-group level mapping
 *       <br>Maps entire slot sharing groups to target TaskManager resource IDs</li>
 * </ol>
 *
 * <p>Expected JSON format:
 * <pre>
 * {
 *   // Subtask-level mappings (checked first)
 *   "operator-name_0": "tm-source",
 *   "operator-name_1": "tm-max",
 *   "operator-name_2": "tm-join",
 *
 *   // Slot-sharing-group level mappings (fallback)
 *   "ingest-group": "tm-source",
 *   "window-max-group": "tm-max"
 * }
 * </pre>
 *
 * <p>The scheduler will first try to find a subtask-level mapping. If not found,
 * it will fall back to the slot-sharing-group level mapping.
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
     * @return A map containing both subtask-level and slot-sharing-group level mappings
     *         from keys (subtask ID or group name) to target TaskManager resource-id
     */
    public Map<String, String> readMigrationPlan() {
        return readMigrationPlan(DEFAULT_MIGRATION_PLAN_PATH);
    }

    /**
     * Reads the migration plan from the specified file path.
     *
     * @param filePath The path to the migration plan JSON file
     * @return A map containing both subtask-level and slot-sharing-group level mappings
     *         from keys (subtask ID or group name) to target TaskManager resource-id
     */
    public Map<String, String> readMigrationPlan(String filePath) {
        Map<String, String> migrationPlan = new HashMap<>();

        File file = new File(filePath);
        if (!file.exists()) {
            LOG.warn("Migration plan file not found at: {}. No resource-id-based slot allocation will be performed.", filePath);
            return migrationPlan;
        }

        try {
            JsonNode rootNode = objectMapper.readTree(file);

            if (rootNode.isObject()) {
                LOG.info("Parsing migration plan entries (supports subtask-level and group-level mappings)...");
                rootNode.fields().forEachRemaining(entry -> {
                    String key = entry.getKey();
                    JsonNode valueNode = entry.getValue();

                    // Skip comment fields
                    if (key.startsWith("_comment")) {
                        return;
                    }

                    if (valueNode.isTextual()) {
                        String targetResourceId = valueNode.asText();
                        migrationPlan.put(key, targetResourceId);

                        // Determine if this is subtask-level or group-level mapping
                        if (key.contains("_") && Character.isDigit(key.charAt(key.length() - 1))) {
                            LOG.info("MigrationPlanReader: [Subtask] [{}] -> Resource-ID [{}]",
                                    key, targetResourceId);
                        } else {
                            LOG.info("MigrationPlanReader: [Group] [{}] -> Resource-ID [{}]",
                                    key, targetResourceId);
                        }
                    } else {
                        LOG.warn("Invalid value format for key {}: expected string, got {}",
                                key, valueNode.getNodeType());
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
     * Gets the target TaskManager resource-id for a specific key (subtask ID or group name).
     *
     * <p>This method is used by the scheduler to look up target resource-ids for both:
     * <ul>
     *   <li>Subtask-level keys (e.g., "operator-name_0", "operator-name_1")</li>
     *   <li>Slot-sharing-group level keys (e.g., "ingest-group", "window-max-group")</li>
     * </ul>
     *
     * @param key The lookup key (subtask ID or slot-sharing-group name)
     * @param migrationPlan The migration plan map
     * @return Target TaskManager resource-id if found, null otherwise
     */
    public static String getTargetIp(String key, Map<String, String> migrationPlan) {
        return migrationPlan.get(key);
    }
}
