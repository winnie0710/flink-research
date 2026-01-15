/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This class implements a {@link SlotSelectionStrategy} that is based on location preference hints.
 */
public abstract class LocationPreferenceSlotSelectionStrategy implements SlotSelectionStrategy {

    LocationPreferenceSlotSelectionStrategy() {}

    @Override
    public Optional<SlotInfoAndLocality> selectBestSlotForProfile(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker, @Nonnull SlotProfile slotProfile) {

        Collection<TaskManagerLocation> locationPreferences = slotProfile.getPreferredLocations();

        if (freeSlotInfoTracker.getAvailableSlots().isEmpty()) {
            return Optional.empty();
        }

        final ResourceProfile resourceProfile = slotProfile.getPhysicalSlotResourceProfile();

        // if we have no location preferences, we can only filter by the additional requirements.
        return locationPreferences.isEmpty()
                ? selectWithoutLocationPreference(freeSlotInfoTracker, resourceProfile)
                : selectWithLocationPreference(
                        freeSlotInfoTracker, locationPreferences, resourceProfile);
    }

    private static final Logger LOG = LoggerFactory.getLogger(LocationPreferenceSlotSelectionStrategy.class);

    @Nonnull
    private Optional<SlotInfoAndLocality> selectWithLocationPreference(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker,
            @Nonnull Collection<TaskManagerLocation> locationPreferences,
            @Nonnull ResourceProfile resourceProfile) {

        // Check if there's a preferred IP in the resource profile
        final Optional<String> preferredIp = resourceProfile.getPreferredIp();

        // If preferred IP is specified, try to find an exact match first
        if (preferredIp.isPresent()) {
            Optional<SlotInfoAndLocality> ipMatchedSlot = 
                    selectByPreferredIp(freeSlotInfoTracker, preferredIp.get(), resourceProfile);
            if (ipMatchedSlot.isPresent()) {
                return ipMatchedSlot;
            }
            // If no exact IP match found, fall through to normal location preference logic
        }

        // we build up two indexes, one for resource id and one for host names of the preferred
        // locations.
        final Map<ResourceID, Integer> preferredResourceIDs =
                CollectionUtil.newHashMapWithExpectedSize(locationPreferences.size());
        final Map<String, Integer> preferredFQHostNames =
                CollectionUtil.newHashMapWithExpectedSize(locationPreferences.size());

        for (TaskManagerLocation locationPreference : locationPreferences) {
            preferredResourceIDs.merge(locationPreference.getResourceID(), 1, Integer::sum);
            preferredFQHostNames.merge(locationPreference.getFQDNHostname(), 1, Integer::sum);
        }

        SlotInfo bestCandidate = null;
        Locality bestCandidateLocality = Locality.UNKNOWN;
        double bestCandidateScore = Double.NEGATIVE_INFINITY;

        for (AllocationID allocationId : freeSlotInfoTracker.getAvailableSlots()) {
            SlotInfo candidate = freeSlotInfoTracker.getSlotInfo(allocationId);

            if (candidate.getResourceProfile().isMatching(resourceProfile)) {

                // this gets candidate is local-weigh
                int localWeigh =
                        preferredResourceIDs.getOrDefault(
                                candidate.getTaskManagerLocation().getResourceID(), 0);

                // this gets candidate is host-local-weigh
                int hostLocalWeigh =
                        preferredFQHostNames.getOrDefault(
                                candidate.getTaskManagerLocation().getFQDNHostname(), 0);

                double candidateScore =
                        calculateCandidateScore(
                                localWeigh,
                                hostLocalWeigh,
                                () -> freeSlotInfoTracker.getTaskExecutorUtilization(candidate));
                if (candidateScore > bestCandidateScore) {
                    bestCandidateScore = candidateScore;
                    bestCandidate = candidate;
                    bestCandidateLocality =
                            localWeigh > 0
                                    ? Locality.LOCAL
                                    : hostLocalWeigh > 0 ? Locality.HOST_LOCAL : Locality.NON_LOCAL;
                }
            }
        }

        // at the end of the iteration, we return the candidate with best possible locality or null.
        return bestCandidate != null
                ? Optional.of(SlotInfoAndLocality.of(bestCandidate, bestCandidateLocality))
                : Optional.empty();
    }

    /**
     * Selects a slot that exactly matches the preferred IP address.
     *
     * @param freeSlotInfoTracker tracker of available slots
     * @param preferredIp the preferred IP address
     * @param resourceProfile the required resource profile
     * @return the selected slot with IP_MATCHED locality, or empty if no match found
     */
    @Nonnull
    private Optional<SlotInfoAndLocality> selectByPreferredIp(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker,
            @Nonnull String preferredIp,
            @Nonnull ResourceProfile resourceProfile) {

        LOG.info("SlotManager: Searching for slot with preferred IP: {}", preferredIp);

        for (AllocationID allocationId : freeSlotInfoTracker.getAvailableSlots()) {
            SlotInfo candidate = freeSlotInfoTracker.getSlotInfo(allocationId);

            if (candidate.getResourceProfile().isMatching(resourceProfile)) {
                String candidateIp = candidate.getTaskManagerLocation().address().getHostAddress();

                LOG.debug("SlotManager: Checking candidate slot on TM: {}", candidateIp);

                // Check for exact IP match
                if (preferredIp.equals(candidateIp)) {
                    LOG.info("SlotManager: ✓ Found matching slot! Allocating slot {} on TaskManager {} (IP: {})", 
                            allocationId, 
                            candidate.getTaskManagerLocation().getResourceID(), 
                            candidateIp);
                    // IP_MATCHED has highest priority (better than LOCAL)
                    return Optional.of(SlotInfoAndLocality.of(candidate, Locality.LOCAL));
                }
            }
        }

        LOG.warn("SlotManager: ✗ No slot found matching preferred IP: {}. Will fall back to normal allocation.", 
                preferredIp);

        return Optional.empty();
    }

    @Nonnull
    protected abstract Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker,
            @Nonnull ResourceProfile resourceProfile);

    protected abstract double calculateCandidateScore(
            int localWeigh, int hostLocalWeigh, Supplier<Double> taskExecutorUtilizationSupplier);

    // -------------------------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------------------------

    public static LocationPreferenceSlotSelectionStrategy createDefault() {
        return new DefaultLocationPreferenceSlotSelectionStrategy();
    }

    public static LocationPreferenceSlotSelectionStrategy createEvenlySpreadOut() {
        return new EvenlySpreadOutLocationPreferenceSlotSelectionStrategy();
    }
}
