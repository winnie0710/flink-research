/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default implementation of {@link RequirementMatcher}. This matcher finds the first requirement
 * that a) is not unfulfilled and B) matches the resource profile.
 */
public class DefaultRequirementMatcher implements RequirementMatcher {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultRequirementMatcher.class);
    @Override
    public Optional<ResourceProfile> match(
            ResourceProfile resourceProfile,
            ResourceCounter totalRequirements,
            Function<ResourceProfile, Integer> numAssignedResourcesLookup) {
        LOG.info("🔍 [MATCHER] Matching offered ResourceProfile: {}", resourceProfile);

        // Step 1: Short-cut for fine-grained resource management. If there is already exactly equal
        // requirement (including preferredIp), we can directly match with it.
        int exactMatchCount = totalRequirements.getResourceCount(resourceProfile);
        int exactMatchAssigned = numAssignedResourcesLookup.apply(resourceProfile);
        LOG.info("🔍 [MATCHER] Exact match check: count={}, assigned={}", exactMatchCount, exactMatchAssigned);

        if (exactMatchCount > exactMatchAssigned) {
            LOG.info("✅ [MATCHER] Found exact match (including preferredIp)!");
            return Optional.of(resourceProfile);
        }

        // Step 2: Priority matching - if offered slot has preferredIp, try to match with
        // requirements that have the same preferredIp first
        String offeredPreferredIp = resourceProfile.getPreferredIp();
        if (offeredPreferredIp != null && !offeredPreferredIp.isEmpty()) {
            LOG.info("🎯 [MATCHER] Offered slot has preferredIp={}, checking for matching requirements",
                    offeredPreferredIp);

            for (Map.Entry<ResourceProfile, Integer> requirementCandidate :
                    totalRequirements.getResourcesWithCount()) {
                ResourceProfile requirementProfile = requirementCandidate.getKey();
                String requiredPreferredIp = requirementProfile.getPreferredIp();

                // Skip if preferredIp doesn't match
                if (requiredPreferredIp == null || !offeredPreferredIp.equals(requiredPreferredIp)) {
                    continue;
                }

                int requiredCount = requirementCandidate.getValue();
                int assignedCount = numAssignedResourcesLookup.apply(requirementProfile);

                LOG.info("🎯 [MATCHER] Found requirement with matching preferredIp={}: {}",
                        offeredPreferredIp, requirementProfile);
                LOG.info("🎯 [MATCHER]   - Required: {}, Assigned: {}, Unfulfilled: {}",
                        requiredCount, assignedCount, requiredCount - assignedCount);

                boolean isMatching = resourceProfile.isMatching(requirementProfile);
                LOG.info("🎯 [MATCHER]   - isMatching: {} (offered.isMatching(required))", isMatching);

                if (isMatching && requiredCount > assignedCount) {
                    LOG.info("✅ [MATCHER] Found preferred match! Requirement with preferredIp={}: {}",
                            offeredPreferredIp, requirementProfile);
                    return Optional.of(requirementProfile);
                } else {
                    if (!isMatching) {
                        LOG.info("❌ [MATCHER]   - Rejected: ResourceProfile not matching");
                    } else {
                        LOG.info("❌ [MATCHER]   - Rejected: No unfulfilled slots (required={}, assigned={})",
                                requiredCount, assignedCount);
                    }
                }
            }

            LOG.info("⚠️ [MATCHER] No unfulfilled requirement found with matching preferredIp={}, falling back to general matching",
                    offeredPreferredIp);
        }

        // Step 3: Fallback - General compatible matching (original logic)
        // Note: This matcher is now primarily used by ResourceManager side matching.
        // JobMaster side uses matchWithOutstandingRequirementAndLocation() which checks TaskManagerLocation.
        LOG.info("🔍 [MATCHER] Performing general compatible matching, checking {} requirement candidates",
                totalRequirements.getResourcesWithCount().size());

        for (Map.Entry<ResourceProfile, Integer> requirementCandidate :
                totalRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirementCandidate.getKey();
            int requiredCount = requirementCandidate.getValue();
            int assignedCount = numAssignedResourcesLookup.apply(requirementProfile);

            LOG.info("🔍 [MATCHER] Checking requirement: {}", requirementProfile);
            LOG.info("🔍 [MATCHER]   - Required: {}, Assigned: {}, Unfulfilled: {}",
                    requiredCount, assignedCount, requiredCount - assignedCount);

            boolean isMatching = resourceProfile.isMatching(requirementProfile);
            LOG.info("🔍 [MATCHER]   - isMatching: {} (offered.isMatching(required))", isMatching);

            // beware the order when matching resources to requirements, because
            // ResourceProfile.UNKNOWN (which only
            // occurs as a requirement) does not match any resource!
            if (isMatching && requiredCount > assignedCount) {
                LOG.info("✅ [MATCHER] Found compatible match! Requirement: {}", requirementProfile);
                return Optional.of(requirementProfile);
            } else {
                if (!isMatching) {
                    LOG.info("❌ [MATCHER]   - Rejected: ResourceProfile not matching");
                } else {
                    LOG.info("❌ [MATCHER]   - Rejected: No unfulfilled slots (required={}, assigned={})",
                            requiredCount, assignedCount);
                }
            }
        }

        LOG.warn("❌ [MATCHER] No matching requirement found for offered ResourceProfile: {}", resourceProfile);
        return Optional.empty();
    }
}
