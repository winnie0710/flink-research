/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link RequestSlotMatchingStrategy} that takes the preferred allocations into account. The
 * strategy will try to fulfill the preferred allocations and if this is not possible, then it will
 * fall back to {@link SimpleRequestSlotMatchingStrategy}.
 */
public enum PreferredAllocationRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger(PreferredAllocationRequestSlotMatchingStrategy.class);

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots, Collection<PendingRequest> pendingRequests) {

        LOG.info("🎯 [MATCHING STRATEGY] Starting to match {} slots with {} pending requests",
                slots.size(), pendingRequests.size());

        final Collection<RequestSlotMatch> requestSlotMatches = new ArrayList<>();

        final Map<AllocationID, PhysicalSlot> freeSlots =
                slots.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlot::getAllocationId, Function.identity()));

        final Map<SlotRequestId, PendingRequest> pendingRequestsWithPreferences =
                new HashMap<>();
        final List<PendingRequest> unmatchedRequests = new ArrayList<>();

        // Split requests into those that have preferences (ResourceID or AllocationID) and those that don't
        for (PendingRequest pendingRequest : pendingRequests) {
            String preferredResourceId = pendingRequest.getResourceProfile().getPreferredIp();
            boolean hasPreferredAllocations = !pendingRequest.getPreferredAllocations().isEmpty();

            if (preferredResourceId != null || hasPreferredAllocations) {
                pendingRequestsWithPreferences.put(
                        pendingRequest.getSlotRequestId(), pendingRequest);
            } else {
                unmatchedRequests.add(pendingRequest);
            }
        }

        // IMPROVED MATCHING LOGIC:
        // For each slot, find a request with matching preferredIp (strict mode)
        final Iterator<PhysicalSlot> freeSlotsIterator = freeSlots.values().iterator();

        while (freeSlotsIterator.hasNext() && !pendingRequestsWithPreferences.isEmpty()) {
            final PhysicalSlot freeSlot = freeSlotsIterator.next();
            final ResourceID slotResourceId = freeSlot.getTaskManagerLocation().getResourceID();
            final String slotPreferredIp = freeSlot.getResourceProfile().getPreferredIp();

            LOG.info("🔍 [MATCH SLOT] Looking for request to match slot {} from TM {} (slot.preferredIp={})",
                    freeSlot.getAllocationId(), slotResourceId, slotPreferredIp);

            boolean slotMatched = false;

            // Try to find a request that wants this specific TaskManager
            final Iterator<PendingRequest> pendingRequestIterator =
                    pendingRequestsWithPreferences.values().iterator();

            while (pendingRequestIterator.hasNext()) {
                final PendingRequest pendingRequest = pendingRequestIterator.next();
                final String requestPreferredIp = pendingRequest.getResourceProfile().getPreferredIp();

                boolean matched = false;

                // STRICT MODE: Request's preferredIp must match slot's TaskManager ResourceID
                if (requestPreferredIp != null) {
                    // Check if request wants this specific TaskManager
                    boolean wantsThisTaskManager = requestPreferredIp.equals(slotResourceId.toString());

                    if (wantsThisTaskManager) {
                        // Also check resource size (CPU, memory)
                        boolean profileMatches = freeSlot.getResourceProfile().isMatching(pendingRequest.getResourceProfile());

                        LOG.info("🔍 [CHECKING] Request {} (preferredIp={}) vs Slot {} (TM={}): wantsThisTaskManager={}, profileMatches={}",
                                pendingRequest.getSlotRequestId(), requestPreferredIp,
                                freeSlot.getAllocationId(), slotResourceId,
                                wantsThisTaskManager, profileMatches);

                        if (profileMatches) {
                            LOG.info("✅ [STRICT MATCH] Slot {} (TM={}) ← Request {} (preferredIp={})",
                                    freeSlot.getAllocationId(), slotResourceId,
                                    pendingRequest.getSlotRequestId(), requestPreferredIp);
                            matched = true;
                        }
                    }
                    // If request wants a different TaskManager, skip (no log to reduce noise)
                } else {
                    // Request has no preferredIp: can match by AllocationID
                    if (freeSlot.getResourceProfile().isMatching(pendingRequest.getResourceProfile())) {
                        if (pendingRequest.getPreferredAllocations().contains(freeSlot.getAllocationId())) {
                            LOG.info("✅ Matched slot {} with request {} by preferred AllocationID",
                                    freeSlot.getAllocationId(), pendingRequest.getSlotRequestId());
                            matched = true;
                        }
                    }
                }

                if (matched) {
                    requestSlotMatches.add(RequestSlotMatch.createFor(pendingRequest, freeSlot));
                    pendingRequestIterator.remove();
                    freeSlotsIterator.remove();
                    slotMatched = true;
                    break;  // Move to next slot
                }
            }

            if (!slotMatched) {
                LOG.debug("⏭️ [NO MATCH] Slot {} from TM {} has no matching request with preferredIp={}",
                        freeSlot.getAllocationId(), slotResourceId, slotResourceId);
            }
        }

        // Separate unmatched requests into those with preferredIp and those without
        final List<PendingRequest> unmatchedRequestsWithPreferredIp = new ArrayList<>();
        final List<PendingRequest> unmatchedRequestsWithoutPreferredIp = new ArrayList<>();

        for (PendingRequest request : pendingRequestsWithPreferences.values()) {
            if (request.getResourceProfile().getPreferredIp() != null) {
                unmatchedRequestsWithPreferredIp.add(request);
            } else {
                unmatchedRequestsWithoutPreferredIp.add(request);
            }
        }

        unmatchedRequests.addAll(unmatchedRequestsWithoutPreferredIp);

        // STRICT MODE: Do NOT allow fallback for requests with preferredIp
        // Only requests WITHOUT preferredIp can use remaining slots
        if (!freeSlots.isEmpty() && !unmatchedRequests.isEmpty()) {
            LOG.info("🔄 Fallback to SimpleStrategy for {} requests without preferredIp using {} remaining slots",
                    unmatchedRequests.size(), freeSlots.size());
            requestSlotMatches.addAll(
                    SimpleRequestSlotMatchingStrategy.INSTANCE.matchRequestsAndSlots(
                            freeSlots.values(), unmatchedRequests));
        }

        // Log unmatched requests with preferredIp (these should wait for correct TaskManager)
        if (!unmatchedRequestsWithPreferredIp.isEmpty()) {
            LOG.info("⏳ {} pending requests with preferredIp are waiting for slots from specific TaskManagers:",
                    unmatchedRequestsWithPreferredIp.size());
            for (PendingRequest request : unmatchedRequestsWithPreferredIp) {
                LOG.info("   - SlotRequestId={}, waiting for TaskManager: {}",
                        request.getSlotRequestId(),
                        request.getResourceProfile().getPreferredIp());
            }
        }

        return requestSlotMatches;
    }

    @Override
    public String toString() {
        return PreferredAllocationRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
