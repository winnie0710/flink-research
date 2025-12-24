package com.github.nexmark.flink.workload;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// K: KeyType (Long), I: InputType (String), O: OutputType (String)
public class RhinoBackgroundWriter extends KeyedProcessFunction<Long, String, String> {

    // 1. 執行器：用於在背景執行 I/O 任務
    private transient ExecutorService backgroundWriter;

    // 2. 狀態：用於確保第一個定時器只註冊一次
    private transient ValueState<Boolean> isTimerSet;

    // 3. 本地寫入路徑：模擬 Rhino 的專用 SSD 存儲路徑
    private final String localReplicationPath = "/path/to/local/ssd/rhino_replica";

    // 4. 複製頻率 (例如 3 分鐘)
    private final long replicationIntervalMs = 3 * 60 * 1000;


    // --- open 方法：初始化資源和狀態 (無 @Override 註解，解決父類問題) ---
    public void open(Configuration parameters) throws Exception {
        // 1. 初始化背景執行器 (解決運行時的 NullPointerException)
        backgroundWriter = Executors.newSingleThreadExecutor();

        // 2. 確保本地複製目錄存在 (TaskManagers 的本地文件系統)
        Files.createDirectories(Paths.get(localReplicationPath));

        // 3. 初始化狀態 (需要 RocksDB 或其他 State Backend 支援)
        isTimerSet = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer-set-flag", Types.BOOLEAN));
    }


    // --- processElement 方法：數據流處理與首次定時器註冊 ---
    // 注意：已移除 throws Exception，因為 IDE 提示沒有拋出檢查型例外
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {

        try {
            // [Task 1: 首次定時器註冊]
            // 檢查定時器是否已設置 (僅對每個 Key 執行一次)
            if (isTimerSet.value() == null || !isTimerSet.value()) {

                // 註冊第一個定時器
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + replicationIntervalMs
                );

                // 標記為已設置
                isTimerSet.update(true);
            }
        } catch (Exception e) {
            // 處理狀態讀取/更新錯誤
            System.err.println("State access error in processElement: " + e.getMessage());
        }

        // 核心處理邏輯：將數據發送到下游
        out.collect("Processed and forwarded: " + value);
    }


    // --- onTimer 方法：週期性複製模擬 ---
    // 注意：已移除 throws Exception
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {

        // [Task 3: 執行非同步寫入]
        // 提交 I/O 任務給背景執行器
        backgroundWriter.submit(() -> {
            // 這個 Lambda 內的程式碼運行在一個與 Flink 主線程不同的線程上
            try {
                // 模擬寫入 50 MB 增量狀態到本地 SSD
                int dummySizeMB = 50;
                byte[] data = new byte[1024];
                // 使用 Key 和 TimeStamp 確保檔案唯一性
                Path filePath = Paths.get(localReplicationPath,
                        "replica_" + ctx.getCurrentKey() + "_" + timestamp);

                try (OutputStream os = new FileOutputStream(filePath.toFile())) {
                    for (int i = 0; i < dummySizeMB * 1024; i++) {
                        os.write(data);
                    }
                }
                System.out.println("Background I/O complete for key: " + ctx.getCurrentKey());

            } catch (Exception e) {
                System.err.println("Background write failed: " + e.getMessage());
            }
        });

        // 註冊下一次定時器，以維持週期性寫入
        ctx.timerService().registerProcessingTimeTimer(timestamp + replicationIntervalMs);
    }


    // --- close 方法：釋放資源 ---
    @Override
    public void close() throws Exception {
        // 在 Operator Subtask 結束時，關閉背景線程
        if (backgroundWriter != null) {
            backgroundWriter.shutdownNow();
        }
        super.close();
    }
}