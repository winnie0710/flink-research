/*
 * Preferred Resource Matching Strategy for CAOM Migration
 *
 * 這個策略會讀取 migration_plan.json 並優先將 subtask 分配到指定的 resource-id
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Resource matching strategy that reads migration plan and prefers allocation
 * to specified resource-id for each subtask.
 */
public class PreferredResourceMatchingStrategy implements ResourceMatchingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PreferredResourceMatchingStrategy.class);

    private static final String MIGRATION_PLAN_PATH = "/opt/flink/plan/migration_plan.json";

    private final ResourceMatchingStrategy fallbackStrategy;
    private Map<String, String> migrationPlan = new HashMap<>();
    private long lastLoadTime = 0;
    private static final long RELOAD_INTERVAL = 5000; // 5 秒重新加載一次

    public PreferredResourceMatchingStrategy(ResourceMatchingStrategy fallbackStrategy) {
        this.fallbackStrategy = fallbackStrategy;
        loadMigrationPlan();
    }

    /**
     * 從 JSON 檔案加載遷移計畫
     */
    private void loadMigrationPlan() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastLoadTime < RELOAD_INTERVAL) {
            return; // 避免頻繁讀取
        }

        try {
            File planFile = new File(MIGRATION_PLAN_PATH);
            if (!planFile.exists()) {
                LOG.debug("Migration plan file not found: {}", MIGRATION_PLAN_PATH);
                return;
            }

            String content = new String(Files.readAllBytes(Paths.get(MIGRATION_PLAN_PATH)));
            ObjectMapper mapper = new ObjectMapper();
            migrationPlan = mapper.readValue(content, Map.class);
            lastLoadTime = currentTime;

            LOG.info("Loaded migration plan with {} entries", migrationPlan.size());
            migrationPlan.forEach((subtask, resourceId) ->
                LOG.debug("  {} -> {}", subtask, resourceId));

        } catch (Exception e) {
            LOG.warn("Failed to load migration plan from {}: {}", MIGRATION_PLAN_PATH, e.getMessage());
        }
    }

    @Override
    public int tryFulfilledRequirementWithResource(
            List<InternalResourceInfo> internalResources,
            int numUnfulfilled,
            ResourceProfile requiredResource,
            JobID jobId) {

        // 重新加載遷移計畫（如果需要）
        loadMigrationPlan();

        if (migrationPlan.isEmpty()) {
            // 如果沒有遷移計畫，使用 fallback 策略
            return fallbackStrategy.tryFulfilledRequirementWithResource(
                    internalResources, numUnfulfilled, requiredResource, jobId);
        }

        // 第一階段：嘗試使用 preferred resource-id 進行分配
        numUnfulfilled = tryAllocateWithPreferredResources(
                internalResources, numUnfulfilled, requiredResource, jobId);

        // 第二階段：對於未滿足的需求，使用 fallback 策略
        if (numUnfulfilled > 0) {
            numUnfulfilled = fallbackStrategy.tryFulfilledRequirementWithResource(
                    internalResources, numUnfulfilled, requiredResource, jobId);
        }

        return numUnfulfilled;
    }

    /**
     * 嘗試使用 preferred resource 進行分配
     */
    private int tryAllocateWithPreferredResources(
            List<InternalResourceInfo> internalResources,
            int numUnfulfilled,
            ResourceProfile requiredResource,
            JobID jobId) {

        Iterator<InternalResourceInfo> iterator = internalResources.iterator();

        while (numUnfulfilled > 0 && iterator.hasNext()) {
            InternalResourceInfo resourceInfo = iterator.next();
            ResourceID resourceId = resourceInfo.getResourceId();

            // 檢查這個 ResourceID 是否在遷移計畫中被指定為目標
            String resourceIdStr = resourceId.getResourceIdString();
            boolean isPreferred = migrationPlan.containsValue(resourceIdStr);

            if (isPreferred) {
                LOG.info("Attempting to allocate on preferred resource: {}", resourceIdStr);

                while (numUnfulfilled > 0
                        && resourceInfo.tryAllocateSlotForJob(jobId, requiredResource)) {
                    numUnfulfilled--;
                    LOG.info("Successfully allocated slot on preferred resource: {}", resourceIdStr);
                }

                if (resourceInfo.availableProfile.equals(ResourceProfile.ZERO)) {
                    iterator.remove();
                }
            }
        }

        return numUnfulfilled;
    }
}
