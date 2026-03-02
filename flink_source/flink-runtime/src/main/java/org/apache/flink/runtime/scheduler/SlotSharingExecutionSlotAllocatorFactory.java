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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/** Factory for {@link SlotSharingExecutionSlotAllocator}. */
public class SlotSharingExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(SlotSharingExecutionSlotAllocatorFactory.class);

    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final SlotSharingStrategy.Factory slotSharingStrategyFactory;

    public SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout) {
        this(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                bulkChecker,
                allocationTimeout,
                new LocalInputPreferredSlotSharingStrategy.Factory());
    }

    SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            SlotSharingStrategy.Factory slotSharingStrategyFactory) {
        this.slotProvider = slotProvider;
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.bulkChecker = bulkChecker;
        this.slotSharingStrategyFactory = slotSharingStrategyFactory;
        this.allocationTimeout = allocationTimeout;
    }

    @Override
    public ExecutionSlotAllocator createInstance(final ExecutionSlotAllocationContext context) {
        SlotSharingStrategy slotSharingStrategy =
                slotSharingStrategyFactory.create(
                        context.getSchedulingTopology(),
                        context.getLogicalSlotSharingGroups(),
                        context.getCoLocationGroups());

        // ✅ Create default retriever
        SyncPreferredLocationsRetriever defaultRetriever =
                new DefaultSyncPreferredLocationsRetriever(context, context);

        // ✅ Wrap with migration plan aware retriever if we can extract SlotPool from slotProvider
        SyncPreferredLocationsRetriever preferredLocationsRetriever;
        SlotPool extractedSlotPool = extractSlotPool(slotProvider);

        if (extractedSlotPool != null) {
            LOG.info("🎯 Using MigrationPlanAwarePreferredLocationsRetriever for migration-based scheduling");
            preferredLocationsRetriever = new MigrationPlanAwarePreferredLocationsRetriever(
                    defaultRetriever,
                    context,
                    extractedSlotPool);
        } else {
            LOG.info("Using default preferred locations retriever (could not extract SlotPool from slotProvider: {})",
                    slotProvider.getClass().getName());
            preferredLocationsRetriever = defaultRetriever;
        }

        SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
                new MergingSharedSlotProfileRetrieverFactory(
                        preferredLocationsRetriever,
                        context::findPriorAllocationId,
                        context::getReservedAllocations);
        return new SlotSharingExecutionSlotAllocator(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                slotSharingStrategy,
                sharedSlotProfileRetrieverFactory,
                bulkChecker,
                allocationTimeout,
                context::getResourceProfile);
    }

    /**
     * Extract SlotPool from PhysicalSlotProvider if possible.
     * Handles PhysicalSlotProviderImpl which wraps a SlotPool internally.
     */
    private static SlotPool extractSlotPool(PhysicalSlotProvider slotProvider) {
        // Direct check for SlotPool
        if (slotProvider instanceof SlotPool) {
            return (SlotPool) slotProvider;
        }

        // Check for PhysicalSlotProviderImpl and extract slotPool field via reflection
        if (slotProvider instanceof PhysicalSlotProviderImpl) {
            try {
                Field slotPoolField = PhysicalSlotProviderImpl.class.getDeclaredField("slotPool");
                slotPoolField.setAccessible(true);
                Object slotPool = slotPoolField.get(slotProvider);

                if (slotPool instanceof SlotPool) {
                    LOG.debug("Successfully extracted SlotPool from PhysicalSlotProviderImpl");
                    return (SlotPool) slotPool;
                }
            } catch (Exception e) {
                LOG.warn("Failed to extract slotPool from PhysicalSlotProviderImpl via reflection", e);
            }
        }

        return null;
    }
}
