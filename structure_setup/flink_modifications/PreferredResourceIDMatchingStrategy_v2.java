/*
 * Preferred ResourceID Matching Strategy for CAOM Migration
 *
 * 使用 Flink 原生的 ResourceID 机制，直接在 ResourceManager 层级进行匹配
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
 * Resource matching strategy that reads migration plan and matches subtasks
 * to TaskManagers based on ResourceID.
 *
 * <p>This strategy uses Flink's native ResourceID mechanism (taskmanager.resource-id)
 * for matching, which is type-safe and officially supported.
 */
public class PreferredResourceIDMatchingStrategy implements ResourceMatchingStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PreferredResourceIDMatchingStrategy.class);

    private static final String MIGRATION_PLAN_PATH = "/opt/flink/plan/migration_plan.json";

    private final ResourceMatchingStrategy fallbackStrategy;
    private Map<String, String> migrationPlan = new HashMap<>();
    private long lastLoadTime = 0;
    private static final long RELOAD_INTERVAL = 5000; // 5 秒重新加載一次

    public PreferredResourceIDMatchingStrategy(ResourceMatchingStrategy fallbackStrategy) {
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

        // ✅ 第一階段：嘗試使用 preferred ResourceID 進行分配
        numUnfulfilled = tryAllocateWithPreferredResourceID(
                internalResources, numUnfulfilled, requiredResource, jobId);

        // 第二階段：對於未滿足的需求，使用 fallback 策略
        if (numUnfulfilled > 0) {
            numUnfulfilled = fallbackStrategy.tryFulfilledRequirementWithResource(
                    internalResources, numUnfulfilled, requiredResource, jobId);
        }

        return numUnfulfilled;
    }

    /**
     * ✅ 使用 Flink 原生 ResourceID 進行分配
     *
     * 這個方法實現你描述的邏輯：
     * 1. 直接使用 ResourceID（Flink原生支援）
     * 2. 從 migration plan 取得 target resource-id
     * 3. 類型安全的比對
     */
    private int tryAllocateWithPreferredResourceID(
            List<InternalResourceInfo> internalResources,
            int numUnfulfilled,
            ResourceProfile requiredResource,
            JobID jobId) {

        // ✅ 從 ResourceProfile 取得 target resource-id
        // 注意：這裡假設 preferred location 存儲在 ResourceProfile 中
        // 實際上可能需要通過其他方式傳遞（如 SlotProfile 或自定義字段）
        String targetResourceId = getTargetResourceIdFromProfile(requiredResource);

        if (targetResourceId == null || targetResourceId.isEmpty()) {
            return numUnfulfilled;  // 沒有 preferred resource，跳過
        }

        Iterator<InternalResourceInfo> iterator = internalResources.iterator();

        while (numUnfulfilled > 0 && iterator.hasNext()) {
            InternalResourceInfo resourceInfo = iterator.next();

            // ✅ 直接使用 ResourceID（Flink原生支援）
            ResourceID taskManagerResourceId = resourceInfo.getTaskExecutorConnection().getResourceID();

            // ✅ 類型安全的比對
            if (targetResourceId.equals(taskManagerResourceId.toString())) {
                LOG.info("Attempting to allocate on preferred TaskManager: {}", taskManagerResourceId);

                while (numUnfulfilled > 0
                        && resourceInfo.tryAllocateSlotForJob(jobId, requiredResource)) {
                    numUnfulfilled--;
                    LOG.info("Successfully allocated slot on preferred TaskManager: {}", taskManagerResourceId);
                }

                if (resourceInfo.availableProfile.equals(ResourceProfile.ZERO)) {
                    iterator.remove();
                }

                // 如果成功在 preferred resource 上分配，提前返回
                if (numUnfulfilled == 0) {
                    break;
                }
            }
        }

        return numUnfulfilled;
    }

    /**
     * 從 migration plan 中獲取目標 ResourceID
     *
     * TODO: 這個方法需要根據實際情況調整
     * 可能的方案：
     * 1. 通過 JobID + subtask index 查找
     * 2. 通過自定義的 ResourceProfile 字段傳遞
     * 3. 通過 AllocationID 映射
     */
    private String getTargetResourceIdFromProfile(ResourceProfile requiredResource) {
        // 方案1: 如果 ResourceProfile 有自定義字段存儲 preferred location
        // return requiredResource.getPreferredLocation();

        // 方案2: 使用 migration plan + 當前分配的上下文信息
        // 這需要更多的上下文信息，如 subtask name/index

        // 臨時方案：從 migration plan 中隨機選一個（僅用於測試）
        if (!migrationPlan.isEmpty()) {
            return migrationPlan.values().iterator().next();
        }

        return null;
    }

    /**
     * Alternative implementation: 直接在 migration plan 中查找
     *
     * 這個版本假設我們可以從某處獲得 subtask identifier
     */
    private int tryAllocateWithMigrationPlan(
            List<InternalResourceInfo> internalResources,
            int numUnfulfilled,
            ResourceProfile requiredResource,
            JobID jobId,
            String subtaskIdentifier) {  // 需要額外的 subtask 識別信息

        // 從 migration plan 查找這個 subtask 應該去哪個 TaskManager
        String targetResourceId = migrationPlan.get(subtaskIdentifier);

        if (targetResourceId == null) {
            return numUnfulfilled;  // 沒有指定目標
        }

        // 找到對應的 TaskManager
        for (InternalResourceInfo resourceInfo : internalResources) {
            ResourceID taskManagerResourceId = resourceInfo.getTaskExecutorConnection().getResourceID();

            if (targetResourceId.equals(taskManagerResourceId.toString())) {
                // 嘗試在這個 TaskManager 上分配
                while (numUnfulfilled > 0
                        && resourceInfo.tryAllocateSlotForJob(jobId, requiredResource)) {
                    numUnfulfilled--;
                    LOG.info("Allocated slot for {} on preferred TaskManager: {}",
                            subtaskIdentifier, taskManagerResourceId);
                }

                if (resourceInfo.availableProfile.equals(ResourceProfile.ZERO)) {
                    internalResources.remove(resourceInfo);
                }

                break;  // 找到目標 TM，無論是否成功都退出
            }
        }

        return numUnfulfilled;
    }
}
