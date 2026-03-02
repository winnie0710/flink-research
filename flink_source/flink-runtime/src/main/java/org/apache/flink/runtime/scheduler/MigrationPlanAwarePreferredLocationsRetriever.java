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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.FreeSlotInfoTracker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link SyncPreferredLocationsRetriever} that integrates migration plan information
 * to provide precise preferred locations for task scheduling.
 *
 * <p>This retriever first checks if there is a migration plan entry for the execution vertex
 * using the {@link ExecutionSlotAllocationContext#getPreferredIp(ExecutionVertexID)} method.
 * If found, it converts the ResourceID from the migration plan to a TaskManagerLocation.
 * If not found or if the TaskManager is not available, it falls back to the default behavior.
 */
class MigrationPlanAwarePreferredLocationsRetriever implements SyncPreferredLocationsRetriever {
    private static final Logger LOG =
            LoggerFactory.getLogger(MigrationPlanAwarePreferredLocationsRetriever.class);

    private final SyncPreferredLocationsRetriever fallbackRetriever;
    private final ExecutionSlotAllocationContext context;
    private final SlotPool slotPool;

    MigrationPlanAwarePreferredLocationsRetriever(
            SyncPreferredLocationsRetriever fallbackRetriever,
            ExecutionSlotAllocationContext context,
            SlotPool slotPool) {
        this.fallbackRetriever = Preconditions.checkNotNull(fallbackRetriever);
        this.context = Preconditions.checkNotNull(context);
        this.slotPool = Preconditions.checkNotNull(slotPool);
    }

    @Override
    public Collection<TaskManagerLocation> getPreferredLocations(
            ExecutionVertexID executionVertexId,
            Set<ExecutionVertexID> producersToIgnore) {

        LOG.info("🔍 getPreferredLocations called for ExecutionVertexID: {}", executionVertexId);

        // ✅ Step 1: Try to get preferred ResourceID from migration plan via context
        String preferredResourceId = context.getPreferredIp(executionVertexId);

        LOG.info("🔍 context.getPreferredIp returned: {}", preferredResourceId);

        if (preferredResourceId != null && !preferredResourceId.isEmpty()) {
            LOG.info("🎯 Found migration plan entry for {}: ResourceID = {}",
                    executionVertexId, preferredResourceId);

            // ✅ Step 2: Convert ResourceID to TaskManagerLocation
            Optional<TaskManagerLocation> location =
                    convertResourceIdToTaskManagerLocation(preferredResourceId);

            if (location.isPresent()) {
                LOG.info("✅ Successfully converted ResourceID {} to TaskManagerLocation: {}",
                        preferredResourceId, location.get());
                return Collections.singleton(location.get());
            } else {
                LOG.warn("⚠️ Could not find TaskManagerLocation for ResourceID: {}. " +
                        "This may happen if the TaskManager hasn't registered yet.",
                        preferredResourceId);
            }
        } else {
            LOG.debug("No migration plan entry found for {}, using fallback retriever",
                    executionVertexId);
        }

        // ✅ Step 3: Fall back to default behavior if migration plan doesn't help
        return fallbackRetriever.getPreferredLocations(executionVertexId, producersToIgnore);
    }

    /**
     * Convert ResourceID string to TaskManagerLocation.
     *
     * <p>In Declarative Slot Pool architecture, TaskManagers may not be present in SlotPool
     * at scheduling time. This method creates a TaskManagerLocation with the target ResourceID,
     * which will be used by slot selection strategy to match against actual slot offers.
     *
     * @param resourceIdString the ResourceID as string
     * @return Optional containing the TaskManagerLocation
     */
    private Optional<TaskManagerLocation> convertResourceIdToTaskManagerLocation(
            String resourceIdString) {
        try {
            ResourceID targetResourceId = new ResourceID(resourceIdString);

            // First, try to find existing TaskManager in SlotPool
            FreeSlotInfoTracker freeSlotTracker = slotPool.getFreeSlotInfoTracker();
            if (freeSlotTracker != null) {
                Collection<SlotInfo> freeSlots = freeSlotTracker.getFreeSlotsInformation();

                for (SlotInfo slotInfo : freeSlots) {
                    TaskManagerLocation location = slotInfo.getTaskManagerLocation();
                    if (location.getResourceID().equals(targetResourceId)) {
                        LOG.info("✅ Found TaskManager {} in free slots", resourceIdString);
                        return Optional.of(location);
                    }
                }
            }

            // Also check allocated slots
            Collection<SlotInfo> allocatedSlots = slotPool.getAllocatedSlotsInformation();
            for (SlotInfo slotInfo : allocatedSlots) {
                TaskManagerLocation location = slotInfo.getTaskManagerLocation();
                if (location.getResourceID().equals(targetResourceId)) {
                    LOG.info("✅ Found TaskManager {} in allocated slots", resourceIdString);
                    return Optional.of(location);
                }
            }

            // If not found in SlotPool, create a placeholder TaskManagerLocation
            // The slot selection strategy will match this ResourceID against actual slot offers
            LOG.info("✅ Creating placeholder TaskManagerLocation for ResourceID: {}", resourceIdString);
            TaskManagerLocation placeholderLocation = new TaskManagerLocation(
                    targetResourceId,
                    java.net.InetAddress.getLoopbackAddress(),  // Placeholder IP
                    -1);  // -1 indicates local/placeholder location

            return Optional.of(placeholderLocation);

        } catch (Exception e) {
            LOG.warn("Failed to create TaskManagerLocation for ResourceID {}: {}",
                    resourceIdString, e.getMessage());
            return Optional.empty();
        }
    }
}
