#!/usr/bin/env bash
# =============================================================================
# run_experiment.sh — Propose vs Baseline 自動化對照實驗腳本
#
# 用法:
#   bash run_experiment.sh both          # 標準流程：propose → 重啟 → baseline
#   bash run_experiment.sh propose       # 只跑 propose（第一階段）
#   bash run_experiment.sh baseline      # 只跑 baseline（需手動指定 run 目錄）
#
# 實驗流程 (both):
#   [Phase 1] propose
#     1. 清空 migration_plan.json（Flink 原生排程決定初始配置）
#     2. compose up → 設定頻寬 → 提交 job
#     3. 啟動 replayer + latency_monitor（CSV 寫入本次 run 目錄）
#     4. 監控 CSV latency，超過閾值 → 背景啟動 auto_propose.py
#     5. auto_propose.py 第一次執行 print_subtask_status() 時，
#        從 Prometheus 讀取當下 Subtask 位置 → 寫入 initial_placement.json
#        （此為 Flink 原生分配的初始配置，尚未被任何遷移修改）
#     6. 10 分鐘後停止，compose down
#   [等待確認]
#     印出 initial_placement.json，等你按 Enter 確認初始配置相同再繼續
#   [Phase 2] baseline
#     1. 清空 migration_plan.json（同 propose 的初始條件）
#     2. compose up → 設定頻寬（完全相同）→ 提交 job
#     3. 啟動 replayer + latency_monitor
#     4. 監控 CSV latency，超過閾值 → 觸發 auto_migrate.py（只一次）
#     5. 10 分鐘後停止，compose down
#
# 輸出結構:
#   results/run_YYYYMMDD_HHMMSS/
#     ├── propose/
#     │   ├── run.log
#     │   ├── experiment_metadata.json
#     │   ├── initial_placement.json     ← 兩次實驗對照用
#     │   ├── bandwidth_config.txt
#     │   ├── latency_data.csv           ← latency_monitor.py 寫入
#     │   ├── latency_monitor.txt        ← stdout 輸出
#     │   ├── replayer.txt
#     │   ├── migration.txt              ← auto_propose.py 輸出
#     │   └── trigger_event.txt          ← 觸發時間點與 latency 值
#     └── baseline/
#         └── （同上結構）
# =============================================================================

set -euo pipefail

# ─── 使用者可調整參數 ─────────────────────────────────────────────────────────
EXPERIMENT_DURATION=600           # 單次實驗總時長（秒）
LATENCY_THRESHOLD=40000             # 觸發啟動遷移程序的 latency 閾值（ms）
RESTART_WAIT=30                   # 兩次實驗之間等待叢集完全關閉的秒數

QUERY="q5"
EXPERIMENT_ID="t19"
REPLAYER_MIN=80000
REPLAYER_MAX=150000

# ── Nexmark Job 設定 ──
MAIN_CLASS="com.github.nexmark.flink.BenchmarkIsoQ5"
FLINK_JAR_PATH="/opt/flink/usrlib/nexmark.jar"
NEXMARK_ROOT_DIR="/opt/nexmark"
PARALLELISM=4
QUERY_NAME="q5-isolated"
CATEGORY_NAME="oa"

# ── 路徑設定 ──
PROJECT_DIR="/home/yenwei/research/structure_setup"
RESULTS_ROOT="${PROJECT_DIR}/results"
VENV_ACTIVATE="${PROJECT_DIR}/venv/bin/activate"
DOCKER_COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
MIGRATION_PLAN_PATH="${PROJECT_DIR}/migration_plan.json"

# ── 初始配置備份路徑（propose 執行時會自動寫入，baseline 執行前由你確認複製）──
# 這個路徑會傳給 auto_propose.py --initial-placement，由 propose_v8.py 在第一次
# auto_detect_and_migrate 時將 Flink 原生分配結果寫入，格式與 migration_plan.json 相同
INITIAL_PLACEMENT_FILENAME="initial_placement.json"   # 相對於 PHASE_DIR

# ── Flink / Docker 設定 ──
FLINK_JOBMANAGER_CONTAINER="jobmanager"
FLINK_REST_URL="http://localhost:8081"

# ── 頻寬限制（5 個 TaskManager，兩次實驗完全相同）──
TM_BW_CONFIGS=(
    "tm-20c-1:300mbit"
    "tm-20c-2:150mbit"
    "tm-20c-3:150mbit"
    "tm-20c-4:300mbit"
    "tm-20c-5:300mbit"
)
BW_BURST="500kb"
BW_LATENCY="50ms"
# ─────────────────────────────────────────────────────────────────────────────

# ─── 全域狀態（每次 run_single_experiment 前重設）────────────────────────────
# RUN_DIR 若已由環境變數傳入（baseline 單獨執行時），保留其值不覆蓋
RUN_DIR="${RUN_DIR:-}"  # results/run_YYYYMMDD_HHMMSS/  ← 兩次實驗共用同一個 run 目錄
PHASE_DIR=""            # RUN_DIR/propose 或 RUN_DIR/baseline
MONITOR_CSV=""          # 本次 phase 的 latency CSV 完整路徑
CHILD_PIDS=()
# ─────────────────────────────────────────────────────────────────────────────

# ─── 工具函式 ─────────────────────────────────────────────────────────────────
log() {
    local msg="[$(date '+%H:%M:%S')] $*"
    echo "$msg"
    [[ -n "${PHASE_DIR:-}" ]] && echo "$msg" >> "${PHASE_DIR}/run.log" 2>/dev/null || true
}

die()  { echo "[ERROR] $*" >&2; exit 1; }
require_cmd() { command -v "$1" &>/dev/null || die "找不到指令: $1"; }

# 在 venv 內後台執行 Python，stdout/stderr tee 到 logfile
pyrun_bg() {
    local logfile="$1"; shift   # 第一個參數是 log 路徑，其餘是 python 指令
    bash -c "
        cd '${PROJECT_DIR}'
        source '${VENV_ACTIVATE}'
        python $*
    " >> "$logfile" 2>&1 &
    CHILD_PIDS+=($!)
}

cleanup() {
    local msg="[$(date '+%H:%M:%S')] === 清理子程序 ==="
    echo "$msg"
    [[ -n "${PHASE_DIR:-}" ]] && echo "$msg" >> "${PHASE_DIR}/run.log" 2>/dev/null || true
    for pid in "${CHILD_PIDS[@]+"${CHILD_PIDS[@]}"}"; do
        kill -0 "$pid" 2>/dev/null && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM
# ─────────────────────────────────────────────────────────────────────────────

# ─── 目錄建立 ────────────────────────────────────────────────────────────────
setup_run_dir() {
    # 只在 both 模式的最開始呼叫一次，產生共用的 run 目錄
    local ts
    ts=$(date '+%Y%m%d_%H%M%S')
    RUN_DIR="${RESULTS_ROOT}/run_${ts}"
    mkdir -p "${RUN_DIR}/propose" "${RUN_DIR}/baseline"
    echo "[$(date '+%H:%M:%S')] 本次實驗根目錄: ${RUN_DIR}"
}

setup_phase_dir() {
    local phase="$1"   # propose | baseline
    PHASE_DIR="${RUN_DIR}/${phase}"
    MONITOR_CSV="${PHASE_DIR}/latency_data.csv"
    log "--- Phase: ${phase} | 輸出目錄: ${PHASE_DIR} ---"
}

# ─── 前置確認 ────────────────────────────────────────────────────────────────
preflight_check() {
    local phase="$1"
    log "=== 前置確認 ==="
    require_cmd docker
    require_cmd curl
    require_cmd python3
    [[ -d "$PROJECT_DIR" ]]         || die "找不到專案目錄: $PROJECT_DIR"
    [[ -f "$VENV_ACTIVATE" ]]       || die "找不到虛擬環境: $VENV_ACTIVATE"
    [[ -f "$DOCKER_COMPOSE_FILE" ]] || die "找不到: $DOCKER_COMPOSE_FILE"
    [[ -f "${PROJECT_DIR}/caom_core/replayer_fast.py" ]]   || die "找不到 replayer_fast.py"
    [[ -f "${PROJECT_DIR}/caom_core/latency_monitor.py" ]] || die "找不到 latency_monitor.py"
    case "$phase" in
        propose)  [[ -f "${PROJECT_DIR}/caom_core/auto_propose.py" ]] || die "找不到 auto_propose.py" ;;
        baseline) [[ -f "${PROJECT_DIR}/caom_core/auto_migrate.py" ]] || die "找不到 auto_migrate.py" ;;
    esac
    log "前置確認通過"
}

# ─── 重設初始配置 ─────────────────────────────────────────────────────────────
reset_migration_plan() {
    log "=== 重設 migration_plan.json → {} ==="
    echo '{}' > "$MIGRATION_PLAN_PATH"
    log "已清空，讓 Flink 原生排程決定初始 Subtask 配置"
}

# ─── 啟動叢集 ────────────────────────────────────────────────────────────────
start_cluster() {
    log "=== 啟動 Docker Compose ==="
    cd "$PROJECT_DIR"

    docker compose -f "$DOCKER_COMPOSE_FILE" down --remove-orphans 2>&1 \
        | tee -a "${PHASE_DIR}/compose_down.txt" || true

    docker compose -f "$DOCKER_COMPOSE_FILE" up -d 2>&1 \
        | tee -a "${PHASE_DIR}/compose_up.txt"

    log "等待 Flink REST API 就緒..."
    local waited=0
    until curl -sf "${FLINK_REST_URL}/overview" &>/dev/null; do
        sleep 3; waited=$((waited + 3))
        [[ $waited -ge 90 ]] && die "Flink REST API 90 秒內未就緒"
    done
    log "Flink REST API 就緒（waited ${waited}s）"
}

# ─── 關閉叢集 ────────────────────────────────────────────────────────────────
stop_cluster() {
    log "=== 關閉 Docker Compose ==="
    cd "$PROJECT_DIR"
    docker compose -f "$DOCKER_COMPOSE_FILE" down 2>&1 \
        | tee -a "${PHASE_DIR}/compose_final_down.txt" || true
    log "叢集已關閉"
}

# ─── 設定頻寬 ────────────────────────────────────────────────────────────────
apply_bandwidth_limits() {
    log "=== 設定 TaskManager 頻寬限制 ==="
    local fail=0
    for entry in "${TM_BW_CONFIGS[@]}"; do
        local container="${entry%%:*}"
        local rate="${entry##*:}"
        log "  ${container} → rate=${rate} burst=${BW_BURST} latency=${BW_LATENCY}"
        docker exec -u root "$container" \
            tc qdisc del dev eth0 root 2>/dev/null || true
        if ! docker exec -u root "$container" \
            tc qdisc add dev eth0 root tbf \
                rate "$rate" burst "$BW_BURST" latency "$BW_LATENCY" \
            2>&1 | tee -a "${PHASE_DIR}/bandwidth_setup.txt"; then
            log "  ⚠️  ${container} 頻寬設定失敗"
            fail=$((fail + 1))
        fi
    done
    printf '%s\n' "${TM_BW_CONFIGS[@]}" > "${PHASE_DIR}/bandwidth_config.txt"
    [[ $fail -gt 0 ]] && die "${fail} 個 TM 頻寬設定失敗，中止實驗"
    log "頻寬設定完成"
}

# ─── 提交 Job ────────────────────────────────────────────────────────────────
# 問題背景：
#   Nexmark 的 BenchmarkIsoQ7 是一個「自帶監控迴圈的 driver」。
#   它提交 job 後不會立即返回，而是繼續在前景等待 metrics。
#   若此時 Kafka 沒有資料（replayer 尚未啟動），driver 等不到 metrics
#   就會自己把 job cancel 掉，造成腳本卡住、replayer 永遠沒機會啟動。
#
# 解法：
#   把整個 docker exec 放到「背景（&）」執行，輸出 tee 到 job_submit.txt，
#   腳本本身只透過 Flink REST API 輪詢 job 是否進入 RUNNING，
#   確認後立刻繼續啟動 replayer + monitor，讓 Kafka 開始有資料進來。
#   Nexmark driver 的監控迴圈繼續在背景跑，不影響實驗流程。
# ─────────────────────────────────────────────────────────────────────────────
submit_job() {
    log "=== 提交 Flink Job（背景執行，不等 driver 返回）==="

    # 背景執行：讓 Nexmark driver 的監控迴圈在背景自行運作
    docker exec -i "$FLINK_JOBMANAGER_CONTAINER" bash -c "
        export NEXMARK_CONF_DIR=${NEXMARK_ROOT_DIR}
        /opt/flink/bin/flink run \
            -d \
            -c ${MAIN_CLASS} \
            -p ${PARALLELISM} \
            ${FLINK_JAR_PATH} \
            --queries ${QUERY_NAME} \
            --location ${NEXMARK_ROOT_DIR} \
            --suite-name 100m \
            --category ${CATEGORY_NAME} \
            --kafka-server kafka:9092 \
            --submit-only
    " >> "${PHASE_DIR}/job_submit.txt" 2>&1 &
    local submit_pid=$!
    CHILD_PIDS+=($submit_pid)
    log "Flink driver PID=${submit_pid}（背景），等待 Job 進入 RUNNING..."

    # 只透過 REST API 確認 job 狀態，不等 driver 前景返回
    local waited=0
    until curl -sf "${FLINK_REST_URL}/jobs" 2>/dev/null | grep -q '"status":"RUNNING"'; do
        sleep 2; waited=$((waited + 2))
        [[ $waited -ge 90 ]] && die "Job 90 秒內未進入 RUNNING，請檢查 job_submit.txt"
    done
    log "Job RUNNING（waited ${waited}s）"
}

# ─── 記錄初始 Subtask 配置 ───────────────────────────────────────────────────
record_initial_placement() {
    log "=== 記錄初始 Subtask 配置 ==="
    local job_id
    job_id=$(curl -sf "${FLINK_REST_URL}/jobs" | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
running = [j for j in jobs if j.get('status') == 'RUNNING']
print(running[0]['id'] if running else '')
" 2>/dev/null || true)

    if [[ -z "$job_id" ]]; then
        log "⚠️  無法取得 job_id，跳過初始配置記錄"
        return
    fi

    curl -sf "${FLINK_REST_URL}/jobs/${job_id}" | python3 -c "
import sys, json, datetime
job = json.load(sys.stdin)
out = {
    'job_id':      job.get('jid', ''),
    'recorded_at': datetime.datetime.now().isoformat(),
    'vertices': [{
        'id':          v.get('id'),
        'name':        v.get('name'),
        'parallelism': v.get('parallelism'),
        'status':      v.get('status'),
    } for v in job.get('vertices', [])]
}
print(json.dumps(out, indent=2, ensure_ascii=False))
" > "${PHASE_DIR}/initial_placement.json" 2>/dev/null || true

    log "初始配置已記錄 → ${PHASE_DIR}/initial_placement.json"
}

# ─── 等待人工確認初始配置 ────────────────────────────────────────────────────
wait_for_placement_confirmation() {
    local propose_placement="${RUN_DIR}/propose/${INITIAL_PLACEMENT_FILENAME}"

    echo ""
    echo "════════════════════════════════════════════════════════════════"
    echo "  [確認] Propose 的初始 Subtask 配置（initial_placement.json）："
    echo "════════════════════════════════════════════════════════════════"
    if [[ -f "$propose_placement" ]]; then
        cat "$propose_placement"
        echo ""
        echo "  ℹ️  格式說明："subtask_key": "tm_id""
        echo "  此檔案由 propose_v8.py 在第一次 auto_detect_and_migrate"
        echo "  執行時，從當時的 migration_plan.json 備份而來（Flink 原生分配結果）。"
    else
        echo "  ⚠️  找不到 ${propose_placement}"
        echo "  可能原因：propose 執行期間從未觸發 auto_detect_and_migrate，"
        echo "  或 migration_plan.json 在 propose 啟動時仍為空。"
        echo ""
        echo "  Baseline 將使用空的 migration_plan.json（{}）啟動。"
        read -r -p "  確認後按 Enter 繼續，或 Ctrl+C 中止... "
        return
    fi
    echo "════════════════════════════════════════════════════════════════"
    echo ""
    echo "  請確認以上配置，然後選擇："
    echo "  [1] 將此配置複製到 migration_plan.json，作為 baseline 的初始配置（推薦）"
    echo "  [2] 使用空的 migration_plan.json（{}），讓 Flink 重新隨機分配"
    echo "  [Ctrl+C] 中止實驗"
    echo ""
    read -r -p "  請輸入選擇 [1/2]（預設 1）: " choice
    choice="${choice:-1}"

    case "$choice" in
        1)
            cp "$propose_placement" "$MIGRATION_PLAN_PATH"
            log "initial_placement.json 已複製 → $MIGRATION_PLAN_PATH"
            echo "  ✅ Baseline 將使用與 Propose 相同的初始配置"
            ;;
        2)
            echo '{}' > "$MIGRATION_PLAN_PATH"
            log "migration_plan.json 已重設為 {}"
            echo "  ✅ Baseline 將由 Flink 原生重新分配"
            ;;
        *)
            echo "  ⚠️  無效輸入，預設使用選項 1"
            cp "$propose_placement" "$MIGRATION_PLAN_PATH"
            log "initial_placement.json 已複製 → $MIGRATION_PLAN_PATH（預設）"
            ;;
    esac
    echo ""
}

# ─── 啟動 Replayer ───────────────────────────────────────────────────────────
start_replayer() {
    log "=== 啟動 Replayer（Kafka 寫入）==="
    pyrun_bg "${PHASE_DIR}/replayer.txt"         "caom_core/replayer_fast.py --min ${REPLAYER_MIN} --max ${REPLAYER_MAX}"
    log "Replayer PID=${CHILD_PIDS[-1]}"
}

# ─── 啟動 Latency Monitor ────────────────────────────────────────────────────
# latency_monitor.py 現在支援 --output-dir 參數，直接指定輸出目錄，
# 不再需要 sed 替換副本。CSV 寫入路徑 = PHASE_DIR/latency_data.csv = MONITOR_CSV
# ─────────────────────────────────────────────────────────────────────────────
start_latency_monitor() {
    log "=== 啟動 Latency Monitor ==="

    bash -c "
        cd '${PROJECT_DIR}'
        source '${VENV_ACTIVATE}'
        python caom_core/latency_monitor.py \
            --query '${QUERY}' \
            --id '${EXPERIMENT_ID}' \
            --output-dir '${PHASE_DIR}'
    " >> "${PHASE_DIR}/latency_monitor.txt" 2>&1 &
    CHILD_PIDS+=($!)
    log "LatencyMonitor PID=${CHILD_PIDS[-1]}，CSV → ${MONITOR_CSV}"
}

# ─── 監控 CSV latency → 閾值觸發啟動 migrate 程序 ───────────────────────────
#
# 設計說明：
#   腳本只負責等 latency 首次超過閾值後「啟動」auto_propose/auto_migrate，
#   啟動後讓程序自己的監控迴圈（每 30 秒檢查、cooldown 300 秒）持續運作，
#   遷移次數由 auto_propose/auto_migrate 自己決定，直到實驗結束才被 cleanup 回收。
# ─────────────────────────────────────────────────────────────────────────────
watch_and_trigger_migration() {
    local phase="$1"
    local migrate_script
    case "$phase" in
        propose)  migrate_script="caom_core/auto_propose.py" ;;
        baseline) migrate_script="caom_core/auto_migrate.py" ;;
    esac

    log "=== 監控 Latency（每 2 秒讀 CSV），閾值=${LATENCY_THRESHOLD} ms ==="
    log "    首次超過閾值後啟動 ${migrate_script}（背景持續運行，可多次遷移）"

    local phase_dir="$PHASE_DIR"
    local monitor_csv="$MONITOR_CSV"

    (
        # 等 CSV 建立（monitor 啟動後約幾秒）
        local waited=0
        while [[ ! -f "$monitor_csv" ]]; do
            sleep 2; waited=$((waited + 2))
            if [[ $waited -ge 60 ]]; then
                echo "[$(date '+%H:%M:%S')] ⚠️  等待 CSV 超時，放棄 latency 監控" \
                    >> "${phase_dir}/run.log"
                exit 0
            fi
        done
        echo "[$(date '+%H:%M:%S')] CSV 就緒，開始監控 latency" \
            >> "${phase_dir}/run.log"

        while true; do
            # 讀 CSV 最後一行的 total_latency_ms，同時與閾值比較
            # 全部在同一個 python3 呼叫內完成，避免 bash 變數插值到 python -c 字串的問題
            local trigger
            trigger=$(python3 -c "
import csv, os, sys
path, threshold = sys.argv[1], float(sys.argv[2])
if not os.path.exists(path): sys.exit(0)
with open(path, newline='') as f:
    rows = list(csv.DictReader(f))
if not rows: sys.exit(0)
v = rows[-1].get('total_latency_ms', '').strip()
if not v: sys.exit(0)
val = float(v)
# 輸出格式: latency值,yes/no
print(f'{val},yes' if val > threshold else f'{val},no')
" "$monitor_csv" "${LATENCY_THRESHOLD}" 2>/dev/null || echo "")

            if [[ -n "$trigger" ]]; then
                local latency exceeded
                latency="${trigger%%,*}"
                exceeded="${trigger##*,}"

                if [[ "$exceeded" == "yes" ]]; then
                    local ts
                    ts=$(date '+%H:%M:%S')
                    echo "[${ts}] ⚡ Latency=${latency} ms > ${LATENCY_THRESHOLD} ms，啟動 ${phase} 遷移程序" \
                        | tee -a "${phase_dir}/run.log"
                    echo "first_trigger_time=${ts}  latency=${latency}  phase=${phase}" \
                        >> "${phase_dir}/trigger_event.txt"

                    local init_placement_arg=""
                    if [[ "${phase}" == "propose" ]]; then
                        init_placement_arg="--initial-placement '${phase_dir}/${INITIAL_PLACEMENT_FILENAME}'"
                    fi

                    # 背景啟動，讓它自己的迴圈持續監控並多次遷移
                    bash -c "
                        cd '${PROJECT_DIR}'
                        source '${VENV_ACTIVATE}'
                        python '${migrate_script}' --query '${QUERY}' --id '${EXPERIMENT_ID}' ${init_placement_arg}
                    " 2>&1 | tee -a "${phase_dir}/migration.txt" &
                    echo $! >> "${phase_dir}/migration_pid.txt"

                    echo "[$(date '+%H:%M:%S')] 遷移程序已啟動（PID=$(cat ${phase_dir}/migration_pid.txt | tail -1)），監控子程序退出" \
                        | tee -a "${phase_dir}/run.log"
                    exit 0   # 監控子 shell 退出，遷移程序繼續背景執行
                fi
            fi
            sleep 2
        done
    ) &
    CHILD_PIDS+=($!)
}

# ─── 等待實驗計時 ────────────────────────────────────────────────────────────
wait_experiment_end() {
    log "=== 計時開始，總時長 ${EXPERIMENT_DURATION}s ==="
    local start end
    start=$(date +%s)
    end=$((start + EXPERIMENT_DURATION))
    while [[ $(date +%s) -lt $end ]]; do
        sleep 30
        log "剩餘 $(( end - $(date +%s) ))s"
    done
    log "=== 時間到 ==="
}

# ─── 寫入實驗 metadata ───────────────────────────────────────────────────────
write_metadata() {
    local phase="$1"
    cat > "${PHASE_DIR}/experiment_metadata.json" <<EOF
{
  "phase":              "${phase}",
  "experiment_id":      "${EXPERIMENT_ID}",
  "query":              "${QUERY}",
  "duration_seconds":   ${EXPERIMENT_DURATION},
  "latency_threshold":  ${LATENCY_THRESHOLD},
  "replayer_min":       ${REPLAYER_MIN},
  "replayer_max":       ${REPLAYER_MAX},
  "parallelism":        ${PARALLELISM},
  "query_name":         "${QUERY_NAME}",
  "category":           "${CATEGORY_NAME}",
  "run_timestamp":      "$(date --iso-8601=seconds)"
}
EOF
}

# ─── 印出本次 phase 結果摘要 ─────────────────────────────────────────────────
print_phase_summary() {
    local phase="$1"
    local trigger_info="（未觸發）"
    if [[ -f "${PHASE_DIR}/trigger_event.txt" ]]; then
        trigger_info=$(cat "${PHASE_DIR}/trigger_event.txt")
    fi

    echo ""
    echo "  ┌─ ${phase} 完成 ─────────────────────────────────────"
    echo "  │  輸出目錄:    ${PHASE_DIR}"
    echo "  │  觸發事件:    ${trigger_info}"
    echo "  │  Latency CSV: ${MONITOR_CSV}"
    echo "  └──────────────────────────────────────────────────────"
    echo ""
}

# ─── 單次 Phase 完整流程 ─────────────────────────────────────────────────────
run_phase() {
    local phase="$1"
    CHILD_PIDS=()

    setup_phase_dir "$phase"
    preflight_check "$phase"
    # migration_plan.json 的內容由呼叫端決定：
    #   propose: reset_migration_plan() 清空為 {}，讓 Flink 原生分配
    #   baseline: wait_for_placement_confirmation() 已處理（複製或清空）
    if [[ "$phase" == "propose" ]]; then
        reset_migration_plan
    fi
    start_cluster
    apply_bandwidth_limits
    submit_job
    record_initial_placement
    start_replayer
    start_latency_monitor
    watch_and_trigger_migration "$phase"
    wait_experiment_end

    log "=== 停止背景程序 ==="
    cleanup
    CHILD_PIDS=()

    stop_cluster
    write_metadata "$phase"
    print_phase_summary "$phase"
}

# ─── 主流程 ──────────────────────────────────────────────────────────────────
main() {
    local mode="${1:-}"
    mkdir -p "$RESULTS_ROOT"

    case "$mode" in
        both)
            # 建立本次實驗共用的 run 目錄
            setup_run_dir

            echo ""
            echo "══════════════════════════════════════════════════════════════"
            echo "  PHASE 1 / 2 — PROPOSE（auto_propose.py）"
            echo "══════════════════════════════════════════════════════════════"
            run_phase propose

            echo ""
            echo "══════════════════════════════════════════════════════════════"
            echo "  叢集冷卻等待 ${RESTART_WAIT}s..."
            echo "══════════════════════════════════════════════════════════════"
            sleep "$RESTART_WAIT"

            # 印出 propose 的初始配置，請使用者確認後再繼續
            wait_for_placement_confirmation

            echo ""
            echo "══════════════════════════════════════════════════════════════"
            echo "  PHASE 2 / 2 — BASELINE（auto_migrate.py）"
            echo "══════════════════════════════════════════════════════════════"
            run_phase baseline

            echo ""
            echo "══════════════════════════════════════════════════════════════"
            echo "  兩次實驗完成。比較資料："
            echo "    Propose  CSV: ${RUN_DIR}/propose/latency_data.csv"
            echo "    Baseline CSV: ${RUN_DIR}/baseline/latency_data.csv"
            echo "    根目錄:       ${RUN_DIR}"
            echo "══════════════════════════════════════════════════════════════"
            ;;

        propose)
            # 只跑第一階段，自動建立 run 目錄
            setup_run_dir
            run_phase propose
            ;;

        baseline)
            # 只跑 baseline，接續已存在的 propose run 目錄
            # 用法 A（環境變數）: RUN_DIR=/path/to/run_xxx bash run_experiment.sh baseline
            # 用法 B（參數）:     bash run_experiment.sh baseline /path/to/run_xxx
            if [[ -n "${2:-}" ]]; then
                RUN_DIR="$2"
            fi
            if [[ -z "${RUN_DIR:-}" ]]; then
                echo "錯誤：baseline 模式需要指定 run 目錄"
                echo ""
                echo "用法 A（建議）:"
                echo "  bash run_experiment.sh baseline /home/yenwei/research/structure_setup/results/run_20260507_154550"
                echo ""
                echo "用法 B（環境變數）:"
                echo "  RUN_DIR=/home/yenwei/.../run_20260507_154550 bash run_experiment.sh baseline"
                exit 1
            fi
            if [[ ! -d "$RUN_DIR" ]]; then
                echo "錯誤：目錄不存在：$RUN_DIR"
                exit 1
            fi
            # 確保 baseline 子目錄存在
            mkdir -p "${RUN_DIR}/baseline"
            log "接續 run 目錄: $RUN_DIR"
            run_phase baseline
            ;;

        *)
            echo "用法: bash $0 both | propose | baseline"
            echo ""
            echo "  both      — 標準流程：propose → 重啟叢集 → baseline（推薦）"
            echo "  propose   — 只跑 propose（第一階段）"
            echo "  baseline  — 只跑 baseline（需設定 RUN_DIR 環境變數接續）"
            exit 1
            ;;
    esac
}

main "$@"