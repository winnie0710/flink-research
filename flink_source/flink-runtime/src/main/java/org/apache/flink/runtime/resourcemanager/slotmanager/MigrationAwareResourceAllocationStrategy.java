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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.util.ResourceCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Migration-aware resource allocation strategy that wraps DefaultResourceAllocationStrategy.
 *
 * <p>This strategy reads a migration plan (JSON file) that maps subtask identifiers to target
 * TaskManager resource IDs. When allocating resources, it tries to place subtasks according
 * to the migration plan, falling back to the default strategy when no preference is specified.
 *
 * <p>Migration plan format:
 * <pre>
 * {
 *   "TaskName_0": "tm_resource_id_1",
 *   "TaskName_1": "tm_resource_id_2",
 *   ...
 * }
 * </pre>
 */
public class MigrationAwareResourceAllocationStrategy implements ResourceAllocationStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationAwareResourceAllocationStrategy.class);

    private static final String MIGRATION_PLAN_PATH = "/opt/flink/plan/migration_plan.json";
    private static final long RELOAD_INTERVAL_MS = 5000; // 5 seconds

    private final ResourceAllocationStrategy defaultStrategy;
    private Map<String, String> migrationPlan = new HashMap<>();
    private long lastLoadTime = 0;

    public MigrationAwareResourceAllocationStrategy(ResourceAllocationStrategy defaultStrategy) {
        this.defaultStrategy = defaultStrategy;
        loadMigrationPlan();
    }

    /**
     * Load migration plan from JSON file.
     */
    private void loadMigrationPlan() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLoadTime < RELOAD_INTERVAL_MS) {
            return; // Avoid frequent reloading
        }

        try {
            File planFile = new File(MIGRATION_PLAN_PATH);
            if (!planFile.exists()) {
                LOG.debug("Migration plan file not found: {}", MIGRATION_PLAN_PATH);
                migrationPlan.clear();
                return;
            }

            // Parse JSON manually to avoid Jackson dependency
            String content = new String(Files.readAllBytes(Paths.get(MIGRATION_PLAN_PATH)));
            Map<String, String> newPlan = parseSimpleJson(content);

            migrationPlan = newPlan;
            lastLoadTime = currentTime;

            LOG.info("✅ Loaded migration plan with {} entries from {}", migrationPlan.size(), MIGRATION_PLAN_PATH);
            if (LOG.isDebugEnabled()) {
                migrationPlan.forEach((subtask, resourceId) ->
                    LOG.debug("  {} -> {}", subtask, resourceId));
            }

        } catch (Exception e) {
            LOG.warn("⚠️ Failed to load migration plan from {}: {}", MIGRATION_PLAN_PATH, e.getMessage());
            migrationPlan.clear();
        }
    }

    /**
     * Simple JSON parser for migration plan (key-value pairs only).
     * Format: { "key1": "value1", "key2": "value2", ... }
     *
     * This parser handles keys with colons in them by using the pattern "key": "value"
     */
    private Map<String, String> parseSimpleJson(String json) {
        Map<String, String> result = new HashMap<>();

        // Remove outer braces and trim
        String cleaned = json.trim();
        if (cleaned.startsWith("{")) {
            cleaned = cleaned.substring(1);
        }
        if (cleaned.endsWith("}")) {
            cleaned = cleaned.substring(0, cleaned.length() - 1);
        }
        cleaned = cleaned.trim();

        if (cleaned.isEmpty()) {
            return result;
        }

        // Use regex to split by comma that's not inside quotes
        String[] pairs = cleaned.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

        for (String pair : pairs) {
            pair = pair.trim();

            // Find the pattern: "key": "value"
            // Split by ": " (colon-space) to separate key and value, but only the LAST occurrence
            // that's outside quotes
            int colonIndex = findLastColonOutsideQuotes(pair);

            if (colonIndex > 0 && colonIndex < pair.length() - 1) {
                String key = pair.substring(0, colonIndex).trim().replace("\"", "");
                String value = pair.substring(colonIndex + 1).trim().replace("\"", "");

                if (!key.isEmpty() && !value.isEmpty()) {
                    result.put(key, value);
                    LOG.debug("Parsed entry: '{}' -> '{}'", key, value);
                }
            }
        }

        return result;
    }

    /**
     * Find the last colon that appears between quotes (the separator between key and value).
     * This handles keys that contain colons.
     */
    private int findLastColonOutsideQuotes(String s) {
        boolean inQuotes = false;
        int lastColonOutsideQuotes = -1;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ':' && !inQuotes) {
                lastColonOutsideQuotes = i;
            }
        }

        return lastColonOutsideQuotes;
    }

    @Override
    public ResourceAllocationResult tryFulfillRequirements(
            Map<JobID, Collection<ResourceRequirement>> missingResources,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        // Reload migration plan if needed
        loadMigrationPlan();

        if (migrationPlan.isEmpty()) {
            // No migration plan, use default strategy
            LOG.debug("No migration plan available, using default strategy");
            return defaultStrategy.tryFulfillRequirements(
                    missingResources, taskManagerResourceInfoProvider, blockedTaskManagerChecker);
        }

        LOG.info("🔄 Using migration-aware allocation strategy with {} planned migrations", migrationPlan.size());

        // First, try to allocate according to migration plan
        ResourceAllocationResult.Builder resultBuilder = ResourceAllocationResult.builder();
        Map<JobID, Collection<ResourceRequirement>> remainingRequirements = new HashMap<>();

        // Build a map of ResourceID -> InstanceID for quick lookup
        Map<String, InstanceID> resourceIdToInstanceId = buildResourceIdMapping(taskManagerResourceInfoProvider);

        LOG.debug("Available TaskManagers: {}", resourceIdToInstanceId.keySet());

        for (Map.Entry<JobID, Collection<ResourceRequirement>> entry : missingResources.entrySet()) {
            JobID jobId = entry.getKey();
            Collection<ResourceRequirement> requirements = entry.getValue();

            Collection<ResourceRequirement> unfulfilled =
                tryAllocateWithMigrationPlan(
                    jobId,
                    requirements,
                    taskManagerResourceInfoProvider,
                    resourceIdToInstanceId,
                    resultBuilder,
                    blockedTaskManagerChecker);

            if (!unfulfilled.isEmpty()) {
                remainingRequirements.put(jobId, unfulfilled);
            }
        }

        // For remaining requirements, use default strategy
        if (!remainingRequirements.isEmpty()) {
            LOG.debug("Using default strategy for {} remaining requirements",
                remainingRequirements.values().stream().mapToInt(Collection::size).sum());

            ResourceAllocationResult defaultResult = defaultStrategy.tryFulfillRequirements(
                    remainingRequirements, taskManagerResourceInfoProvider, blockedTaskManagerChecker);

            // Merge results
            return mergeResults(resultBuilder, defaultResult);
        }

        return resultBuilder.build();
    }

    /**
     * Build mapping from ResourceID string to InstanceID.
     */
    private Map<String, InstanceID> buildResourceIdMapping(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {

        Map<String, InstanceID> mapping = new HashMap<>();

        for (TaskManagerInfo tmInfo : taskManagerResourceInfoProvider.getRegisteredTaskManagers()) {
            ResourceID resourceId = tmInfo.getTaskExecutorConnection().getResourceID();
            InstanceID instanceId = tmInfo.getInstanceId();
            mapping.put(resourceId.toString(), instanceId);

            LOG.debug("Mapped ResourceID {} -> InstanceID {}", resourceId, instanceId);
        }

        return mapping;
    }

    /**
     * Try to allocate resources according to migration plan.
     * Returns unfulfilled requirements.
     *
     * This method processes requirements that have preferredIp set (via migration plan).
     * It allocates slots from the TaskManager specified by preferredIp.
     */
    private Collection<ResourceRequirement> tryAllocateWithMigrationPlan(
            JobID jobId,
            Collection<ResourceRequirement> requirements,
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider,
            Map<String, InstanceID> resourceIdToInstanceId,
            ResourceAllocationResult.Builder resultBuilder,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {

        Collection<ResourceRequirement> unfulfilled = new ArrayList<>();

        for (ResourceRequirement requirement : requirements) {
            int numRequired = requirement.getNumberOfRequiredSlots();
            ResourceProfile resourceProfile = requirement.getResourceProfile();
            String requiredPreferredIp = resourceProfile.getPreferredIp();

            LOG.info("🔍 [RM ALLOCATION] Processing requirement: {} slots with preferredIp={}",
                    numRequired, requiredPreferredIp);

            // Check if requirement has preferredIp specified
            if (requiredPreferredIp == null || requiredPreferredIp.isEmpty()) {
                LOG.debug("Requirement has no preferredIp, will use default strategy");
                unfulfilled.add(requirement);
                continue;
            }

            // Get the target TaskManager by preferredIp
            InstanceID targetInstanceId = resourceIdToInstanceId.get(requiredPreferredIp);
            if (targetInstanceId == null) {
                LOG.warn("❌ [RM ALLOCATION] Target TaskManager with ResourceID {} not found in registered TaskManagers",
                        requiredPreferredIp);
                LOG.warn("❌ [RM ALLOCATION] Available TaskManagers: {}", resourceIdToInstanceId.keySet());
                unfulfilled.add(requirement);
                continue;
            }

            // Get TaskManager info
            TaskManagerInfo tmInfo = taskManagerResourceInfoProvider.getRegisteredTaskManager(targetInstanceId).orElse(null);
            if (tmInfo == null) {
                LOG.warn("❌ [RM ALLOCATION] TaskManager info not found for InstanceID {}", targetInstanceId);
                unfulfilled.add(requirement);
                continue;
            }

            // Check if target TaskManager is blocked
            ResourceID tmResourceId = tmInfo.getTaskExecutorConnection().getResourceID();
            if (blockedTaskManagerChecker.isBlockedTaskManager(tmResourceId)) {
                LOG.warn("❌ [RM ALLOCATION] Target TaskManager {} is blocked", tmResourceId);
                unfulfilled.add(requirement);
                continue;
            }

            // Try to allocate all required slots from this TaskManager
            int allocated = 0;
            ResourceProfile availableResource = tmInfo.getAvailableResource();

            LOG.info("🔍 [RM ALLOCATION] Target TaskManager {} available resource: {}",
                    requiredPreferredIp, availableResource);

            // Calculate how many slots can be allocated
            // We need to check if TaskManager has enough resources for each slot
            ResourceProfile currentAvailable = availableResource;
            for (int i = 0; i < numRequired; i++) {
                if (currentAvailable.allFieldsNoLessThan(resourceProfile)) {
                    // Allocate one slot
                    resultBuilder.addAllocationOnRegisteredResource(jobId, targetInstanceId, resourceProfile);
                    allocated++;

                    // Subtract allocated resources from available
                    currentAvailable = currentAvailable.subtract(resourceProfile);

                    LOG.debug("✅ Allocated slot {}/{} on TaskManager {}", i + 1, numRequired, requiredPreferredIp);
                } else {
                    LOG.warn("❌ [RM ALLOCATION] TaskManager {} doesn't have enough resources for slot {}/{}",
                            requiredPreferredIp, i + 1, numRequired);
                    LOG.warn("❌ [RM ALLOCATION] Required: {}, Available: {}", resourceProfile, currentAvailable);
                    break;
                }
            }

            if (allocated > 0) {
                LOG.info("✅ [RM ALLOCATION] Allocated {}/{} slots on preferred TaskManager {} for JobID {}",
                        allocated, numRequired, requiredPreferredIp, jobId);
            }

            // If not all slots could be allocated, add remaining to unfulfilled
            if (allocated < numRequired) {
                int remaining = numRequired - allocated;
                unfulfilled.add(ResourceRequirement.create(resourceProfile, remaining));
                LOG.warn("⚠️ [RM ALLOCATION] Could not allocate all slots: {} remaining for preferredIp={}",
                        remaining, requiredPreferredIp);
            }
        }

        return unfulfilled;
    }

    /**
     * Merge results from migration-aware allocation and default strategy.
     */
    private ResourceAllocationResult mergeResults(
            ResourceAllocationResult.Builder migrationBuilder,
            ResourceAllocationResult defaultResult) {

        // Add unfulfillable jobs
        for (JobID jobId : defaultResult.getUnfulfillableJobs()) {
            migrationBuilder.addUnfulfillableJob(jobId);
        }

        // Add allocations on registered resources
        for (Map.Entry<JobID, Map<InstanceID, ResourceCounter>> jobEntry :
                defaultResult.getAllocationsOnRegisteredResources().entrySet()) {
            JobID jobId = jobEntry.getKey();
            for (Map.Entry<InstanceID, ResourceCounter> tmEntry : jobEntry.getValue().entrySet()) {
                InstanceID instanceId = tmEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> resourceEntry :
                        tmEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < resourceEntry.getValue(); i++) {
                        migrationBuilder.addAllocationOnRegisteredResource(
                            jobId, instanceId, resourceEntry.getKey());
                    }
                }
            }
        }

        // Add pending task managers
        for (PendingTaskManager pendingTM : defaultResult.getPendingTaskManagersToAllocate()) {
            migrationBuilder.addPendingTaskManagerAllocate(pendingTM);
        }

        // Add allocations on pending resources
        for (Map.Entry<PendingTaskManagerId, Map<JobID, ResourceCounter>> pendingEntry :
                defaultResult.getAllocationsOnPendingResources().entrySet()) {
            PendingTaskManagerId pendingId = pendingEntry.getKey();
            for (Map.Entry<JobID, ResourceCounter> jobEntry : pendingEntry.getValue().entrySet()) {
                JobID jobId = jobEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> resourceEntry :
                        jobEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < resourceEntry.getValue(); i++) {
                        migrationBuilder.addAllocationOnPendingResource(
                            jobId, pendingId, resourceEntry.getKey());
                    }
                }
            }
        }

        return migrationBuilder.build();
    }

    @Override
    public ResourceReconcileResult tryReconcileClusterResources(
            TaskManagerResourceInfoProvider taskManagerResourceInfoProvider) {
        // Delegate to default strategy for cluster reconciliation
        return defaultStrategy.tryReconcileClusterResources(taskManagerResourceInfoProvider);
    }
}
