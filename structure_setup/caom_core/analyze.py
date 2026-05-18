#!/usr/bin/env python3
"""
analyze.py — 實驗結果分析腳本
用途：跑完所有實驗後，掃描 output/ 目錄，自動產生三個模組的分析結果與圖表。

使用方式：
    python3 analyze.py --output-root ./output --replayer-log ./replayer.txt

    --mode 可省略，腳本會從 replayer.txt 自動偵測模式。
    scenario 模式下視窗邊界自動從 replayer.txt 的 Real TPS 決定：
      window_start = Real TPS 第一次超過 tps_max * 50% 的時刻
      window_end   = Real TPS 最後一次低於 tps_max * 50% 的時刻

輸出：
    analysis_module1_bottleneck.csv   模組一：瓶頸偵測準確率
    analysis_module2_throughput.csv   模組二：同 N 筆資料處理時間比較
    analysis_module3_overhead.csv     模組三：遷移開銷統計
    figures/                          所有圖表
"""

import os
import re
import glob
import argparse
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path


# ══════════════════════════════════════════════════════════════════════════════
# 工具函式
# ══════════════════════════════════════════════════════════════════════════════

def parse_replayer_log(replayer_log_path: str) -> dict:
    """
    解析 replayer.txt，取出三種資訊：
    1. keyframe 劇本表：[(elapsed_sec, tps), ...]  （scenario 模式專用）
    2. timeline：[(elapsed_sec, real_tps), ...]    （所有模式均有，每 3 秒一筆）
    3. 基本參數：mode, tps_min, tps_max

    timeline 的 Real TPS 是實際輸出值，比 keyframe 更精確，
    用於 sine / composite / scenario 的視窗邊界自動偵測。

    回傳：
    {
        "keyframes": [(0, 60000), (30, 70000), ...],
        "timeline":  [(0, 60000), (3, 61000), ...],
        "mode": "triple_peak" | "sine" | "composite" | "unknown",
        "tps_min": 60000,
        "tps_max": 140000,
    }
    """
    keyframes = []
    timeline  = []
    mode      = "unknown"
    tps_min, tps_max = None, None

    keyframe_pattern = re.compile(r'^\s*(\d+)s\s+([\d,]+)\s+\d+%')
    mode_pattern     = re.compile(r'ScenarioModel 劇本摘要（(\S+)）')
    range_pattern    = re.compile(r'TPS 範圍：([\d,]+)\s*~\s*([\d,]+)')
    # ⏱️  60s | Mode: scenario | Target: 98k | Real: 97k | Total: 4,325,000
    # 也相容 sine/composite 不含 ScenarioModel 的格式
    timeline_pattern = re.compile(r'(\d+)s\s*\|.*?Real:\s*([\d,]+)k')

    # sine / composite 模式從 TPS 模式描述行推斷
    sine_pattern      = re.compile(r'TPS 模式[：:]\s*.*正弦')
    composite_pattern = re.compile(r'TPS 模式[：:]\s*.*複合')
    random_pattern    = re.compile(r'TPS 模式[：:]\s*.*隨機')
    bursty_pattern    = re.compile(r'TPS 模式[：:]\s*.*尖峰')

    seen_secs = set()

    with open(replayer_log_path, 'r', encoding='utf-8') as f:
        for line in f:
            # 模式偵測
            m = mode_pattern.search(line)
            if m:
                mode = m.group(1)   # e.g. "double_peak", "triple_peak"

            if mode == "unknown":
                if sine_pattern.search(line):
                    mode = "sine"
                elif composite_pattern.search(line):
                    mode = "composite"
                elif random_pattern.search(line):
                    mode = "random"
                elif bursty_pattern.search(line):
                    mode = "bursty"

            # TPS 範圍
            m = range_pattern.search(line)
            if m:
                tps_min = int(m.group(1).replace(',', ''))
                tps_max = int(m.group(2).replace(',', ''))

            # keyframe 劇本表（scenario 專用）
            m = keyframe_pattern.match(line)
            if m:
                keyframes.append((int(m.group(1)), int(m.group(2).replace(',', ''))))

            # timeline：每 3 秒一筆 Real TPS，去重保留第一筆
            m = timeline_pattern.search(line)
            if m:
                sec      = int(m.group(1))
                real_tps = int(m.group(2).replace(',', '')) * 1000
                if sec not in seen_secs:
                    seen_secs.add(sec)
                    timeline.append((sec, real_tps))

    timeline.sort(key=lambda x: x[0])

    return {
        "keyframes": keyframes,
        "timeline":  timeline,
        "mode":      mode,
        "tps_min":   tps_min,
        "tps_max":   tps_max,
    }


def get_window_bounds(replayer_info: dict, effective_mode: str) -> tuple[int, int]:
    """
    根據 replayer.txt 的 Real TPS timeline 自動決定分析視窗邊界。
    三種模式（scenario / sine / composite）統一用同一邏輯：

      window_start = Real TPS 第一次 >= tps_max * 50% 的時刻
      window_end   = Real TPS 最後一次 <  tps_max * 50% 的時刻

    這對應「第一次進入高壓段 → 最後一次離開高壓段」，
    包含所有峰值與中間緩和段，是保守且可辯護的視窗選取方式。

    sine 模式若最低點仍高於 50%（即 offset - amplitude >= tps_max * 0.5），
    則門檻自動提升至 tps_min + (tps_max - tps_min) * 0.3，
    確保能找到「相對低點」作為 window_end。
    """
    tps_max  = replayer_info.get("tps_max") or 1
    tps_min  = replayer_info.get("tps_min") or 0
    timeline = replayer_info.get("timeline", [])

    if not timeline:
        # timeline 解析失敗，使用最終 fallback
        print("  ⚠️  timeline 解析失敗，使用預設視窗 180s ~ 720s")
        return (180, 720)

    # 決定門檻：預設 50%，sine 模式若最低點高於 50% 則提升門檻
    threshold_pct = 0.50
    sine_min_tps  = tps_min  # sine 的理論最低點就是 tps_min
    if effective_mode == "sine" and sine_min_tps >= tps_max * threshold_pct:
        # sine 永遠不低於 50%，改用「高於平均值的 30%」作為門檻
        threshold_pct = (tps_min + (tps_max - tps_min) * 0.30) / tps_max
        print(f"  ℹ️  sine 模式：最低 TPS ({tps_min:,}) >= tps_max * 50%，"
              f"門檻調整為 {threshold_pct*100:.0f}%")

    threshold = tps_max * threshold_pct

    # window_start：Real TPS 第一次 >= threshold 的時刻
    window_start = None
    for sec, tps in timeline:
        if tps >= threshold:
            window_start = sec
            break

    # window_end：Real TPS 最後一次 < threshold 的時刻（從後往前找）
    window_end = None
    for sec, tps in reversed(timeline):
        if tps < threshold:
            window_end = sec
            break

    # 合理性檢查
    if window_start is None:
        window_start = timeline[0][0]
        print(f"  ⚠️  找不到 window_start（Real TPS 從未超過門檻），使用實驗起點 {window_start}s")

    if window_end is None or window_end <= window_start:
        window_end = timeline[-1][0]
        print(f"  ⚠️  找不到有效 window_end，使用實驗終點 {window_end}s")

    return (window_start, window_end)


def parse_migration_record(record_path: str) -> list[dict]:
    """
    解析 migration_record.txt，回傳每次遷移的結構化資料。

    回傳格式：
    [
        {
            "event_datetime": "2026-05-10 16:14:25",
            "event_timestamp": 1746864865.0,   # 從 datetime 轉換
            "job_id_before": "e969...",
            "job_id_after":  "7f2f...",
            "total_downtime_sec": 6.160,
            "savepoint_sec":  2.018,
            "wait_sec":       0.002,
            "restart_sec":    4.139,
            "subtasks": [
                {
                    "subtask_id":    "Window_Join_1",
                    "from_tm":       "tm_20c_2",
                    "to_tm":         "tm_20c_2",
                    "state_size_bytes": 71303168,   # 67.99 MB
                    "bottleneck_cause": "NETWORK_BOTTLENECK",
                    "actually_moved": False,
                }
            ],
        },
        ...
    ]
    """
    import datetime

    migrations = []

    if not os.path.exists(record_path):
        return migrations

    with open(record_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 用 === 分割每一次遷移區塊
    blocks = re.split(r'={10,}', content)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        m_time = re.search(r'遷移時間:\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})', block)
        m_jbefore = re.search(r'Job ID \(遷移前\):\s*(\S+)', block)
        m_jafter  = re.search(r'Job ID \(遷移後\):\s*(\S+)', block)
        m_total   = re.search(r'中斷時間總計:\s*([\d.]+)s', block)
        m_save    = re.search(r'Savepoint\s*:\s*([\d.]+)s', block)
        m_wait    = re.search(r'Wait\s*:\s*([\d.]+)s', block)
        m_restart = re.search(r'Restart\s*:\s*([\d.]+)s', block)

        if not (m_time and m_total):
            continue

        dt_str = m_time.group(1)
        try:
            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            ts = dt.timestamp()
        except Exception:
            ts = 0.0

        entry = {
            "event_datetime":    dt_str,
            "event_timestamp":   ts,
            "job_id_before":     m_jbefore.group(1) if m_jbefore else "N/A",
            "job_id_after":      m_jafter.group(1)  if m_jafter  else "N/A",
            "total_downtime_sec": float(m_total.group(1)),
            "savepoint_sec":      float(m_save.group(1))    if m_save    else 0.0,
            "wait_sec":           float(m_wait.group(1))    if m_wait    else 0.0,
            "restart_sec":        float(m_restart.group(1)) if m_restart else 0.0,
            "subtasks":           [],
        }

        # 解析 Subtask 明細行
        # 格式：  Window_Join_2   tm_20c_2   tm_20c_3   67.14 MB   NETWORK_BOTTLENECK
        subtask_pattern = re.compile(
            r'^\s{2}(\S+)\s+(\S+)\s+(\S+)\s+([\d.]+\s+[KMGBT]?B|N/A)\s+(\S+)',
            re.MULTILINE
        )
        for sm in subtask_pattern.finditer(block):
            sid        = sm.group(1)
            from_tm    = sm.group(2)
            to_tm      = sm.group(3)
            size_str   = sm.group(4).strip()
            cause      = sm.group(5)

            # 過濾掉 header 行
            if sid in ("Subtask", "---"):
                continue
            if "ID" in sid and "Before" in from_tm:
                continue

            # 解析 state size → bytes
            state_bytes = _parse_size_to_bytes(size_str)

            entry["subtasks"].append({
                "subtask_id":       sid,
                "from_tm":          from_tm,
                "to_tm":            to_tm,
                "state_size_bytes": state_bytes,
                "bottleneck_cause": cause,
                "actually_moved":   from_tm != to_tm,
            })

        migrations.append(entry)

    return migrations


def _parse_size_to_bytes(size_str: str) -> int:
    """將 '67.99 MB' / '512.00 KB' / 'N/A' 轉為 bytes"""
    if not size_str or size_str.strip() == "N/A":
        return 0
    m = re.match(r'([\d.]+)\s*([KMGBT]?B)', size_str.strip(), re.IGNORECASE)
    if not m:
        return 0
    value = float(m.group(1))
    unit  = m.group(2).upper()
    multipliers = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    return int(value * multipliers.get(unit, 1))


def load_latency_csv(csv_path: str) -> pd.DataFrame:
    """載入 latency_data.csv，確保欄位型別正確"""
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    df = pd.read_csv(csv_path)
    df['unix_timestamp']  = df['unix_timestamp'].astype(float)
    df['relative_time']   = df['relative_time'].astype(float)
    df['total_latency_ms'] = pd.to_numeric(df['total_latency_ms'], errors='coerce')
    df['throughput']       = pd.to_numeric(df['throughput'], errors='coerce').fillna(0.0)
    return df.sort_values('unix_timestamp').reset_index(drop=True)


def scan_experiments(output_root: str) -> list[dict]:
    """
    掃描 output_root 下的所有子目錄，回傳實驗清單。
    每個實驗目錄需包含 latency_data.csv，才算有效。

    回傳格式：
    [
        {
            "experiment_id": "baseline_q5_double_peak_t01",
            "method": "baseline",   # 從目錄名推斷（含 baseline → baseline，否則 propose）
            "dir": "/path/to/output/baseline_q5_...",
            "latency_csv": "...",
            "migration_record": "...",
        },
        ...
    ]
    """
    experiments = []
    for subdir in sorted(Path(output_root).iterdir()):
        if not subdir.is_dir():
            continue
        latency_csv = subdir / "latency_data.csv"
        if not latency_csv.exists():
            continue

        exp_id = subdir.name
        method = "baseline" if "baseline" in exp_id.lower() else "propose"

        experiments.append({
            "experiment_id":   exp_id,
            "method":          method,
            "dir":             str(subdir),
            "latency_csv":     str(latency_csv),
            "migration_record": str(subdir / "migration_record.txt"),
        })

    return experiments


# ══════════════════════════════════════════════════════════════════════════════
# 模組一：瓶頸偵測準確率
# ══════════════════════════════════════════════════════════════════════════════

def analyze_module1(experiments: list[dict], improvement_threshold_pct: float = 10.0,
                    after_window_sec: float = 60.0,
                    after_skip_sec: float = 5.0,
                    fn_recovery_grace_sec: float = 120.0) -> pd.DataFrame:
    """
    對每一次遷移事件判斷：TP / FP / FN

    TP：遷移觸發前 10 秒內的 latency（before）比 throughput 恢復後
        after_skip_sec ~ after_window_sec 的 latency（after）下降 >= improvement_threshold_pct%
    FP：下降幅度不足（誤判，遷移沒有改善 latency）
    FN：latency 持續高（超過全局中位數 1.5 倍，連續 >= 60s）
        且不在任何遷移事件的恢復期（遷移完成後 fn_recovery_grace_sec 秒）內

    Before 取「觸發前 10 秒、throughput > 0」的最後幾筆，
    代表遷移當下系統真實承受的 latency 峰值。

    After 取「throughput 恢復 > 0 的第一個時刻」之後的穩定段，
    跳過 job 重啟後的積壓爆衝期（爆衝期 throughput 仍為 0）。
    """
    rows = []

    for exp in experiments:
        latency_df    = load_latency_csv(exp["latency_csv"])
        migrations    = parse_migration_record(exp["migration_record"])
        experiment_id = exp["experiment_id"]
        method        = exp["method"]

        if latency_df.empty:
            continue

        exp_start_ts = latency_df['unix_timestamp'].iloc[0]

        # ── TP / FP 判斷 ──────────────────────────────────────────────────────
        for mig in migrations:
            t_trigger = mig["event_timestamp"]
            t_done    = t_trigger + mig["total_downtime_sec"]

            # Before：觸發前 10 秒內、throughput > 0 的最後幾筆
            # 代表遷移當下系統真實的 latency 峰值
            before_raw = latency_df[
                (latency_df['unix_timestamp'] >= t_trigger - 10) &
                (latency_df['unix_timestamp'] <  t_trigger) &
                (latency_df['throughput'] > 0)
                ]['total_latency_ms'].dropna()

            # fallback：若取樣點不足，放寬到前 20 秒且不過濾 throughput
            if len(before_raw) < 2:
                before_raw = latency_df[
                    (latency_df['unix_timestamp'] >= t_trigger - 20) &
                    (latency_df['unix_timestamp'] <  t_trigger)
                    ]['total_latency_ms'].dropna()

            # After：等 job 重啟且 throughput 恢復 > 0 後再觀察
            # 跳過重啟後的積壓爆衝期（throughput 仍為 0 的那段）
            after_candidates = latency_df[
                (latency_df['unix_timestamp'] > t_done) &
                (latency_df['throughput'] > 0)
                ]

            if after_candidates.empty:
                continue

            t_recovery = after_candidates['unix_timestamp'].iloc[0]

            after = latency_df[
                (latency_df['unix_timestamp'] >= t_recovery + after_skip_sec) &
                (latency_df['unix_timestamp'] <= t_recovery + after_window_sec) &
                (latency_df['throughput'] > 0)
                ]['total_latency_ms'].dropna()

            if before_raw.empty or after.empty:
                continue

            before_median   = before_raw.median()
            after_median    = after.median()
            improvement_pct = (before_median - after_median) / before_median * 100 \
                if before_median > 0 else 0.0
            result = "TP" if improvement_pct >= improvement_threshold_pct else "FP"

            moved  = [s for s in mig["subtasks"] if s["actually_moved"]]
            causes = list({s["bottleneck_cause"] for s in moved})

            rows.append({
                "experiment_id":       experiment_id,
                "method":              method,
                "event_datetime":      mig["event_datetime"],
                "t_migration_trigger": round(t_trigger - exp_start_ts, 1),
                "t_migration_done":    round(t_done    - exp_start_ts, 1),
                "t_recovery":          round(t_recovery - exp_start_ts, 1),
                "before_latency_ms":   round(before_median, 1),
                "after_latency_ms":    round(after_median, 1),
                "improvement_pct":     round(improvement_pct, 1),
                "result":              result,
                "moved_subtask_count": len(moved),
                "bottleneck_causes":   ";".join(causes),
            })

        # ── FN 偵測 ───────────────────────────────────────────────────────────
        # 找「latency 持續高但無遷移事件（且不在恢復期內）」的時間段
        global_median  = latency_df['total_latency_ms'].median()
        high_threshold = global_median * 1.5
        SAMPLE_INTERVAL = 2
        MIN_CONSECUTIVE = int(60 / SAMPLE_INTERVAL)   # 連續 60 秒

        is_high  = latency_df['total_latency_ms'] > high_threshold
        group_id = (is_high != is_high.shift()).cumsum()

        for gid, group in latency_df[is_high].groupby(group_id[is_high]):
            if len(group) < MIN_CONSECUTIVE:
                continue

            seg_start_ts = group['unix_timestamp'].iloc[0]
            seg_end_ts   = group['unix_timestamp'].iloc[-1]
            seg_duration = seg_end_ts - seg_start_ts

            # 排除：時間段與任何遷移事件重疊，或落在遷移完成後的恢復期內
            in_migration_or_recovery = any(
                seg_start_ts <= (mig["event_timestamp"] + mig["total_downtime_sec"]
                                 + fn_recovery_grace_sec)
                and seg_end_ts >= mig["event_timestamp"]
                for mig in migrations
            )

            if not in_migration_or_recovery:
                rows.append({
                    "experiment_id":       experiment_id,
                    "method":              method,
                    "event_datetime":      "",
                    "t_migration_trigger": None,
                    "t_migration_done":    group['relative_time'].iloc[0],
                    "t_recovery":          None,
                    "before_latency_ms":   round(group['total_latency_ms'].median(), 1),
                    "after_latency_ms":    None,
                    "improvement_pct":     None,
                    "result":              "FN",
                    "moved_subtask_count": 0,
                    "bottleneck_causes":   f"NO_MIGRATION (latency high {seg_duration:.0f}s)",
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return df, pd.DataFrame()

    summary = df.groupby(["experiment_id", "method"])["result"].value_counts().unstack(fill_value=0)
    for col in ["TP", "FP", "FN"]:
        if col not in summary.columns:
            summary[col] = 0
    summary["precision"] = (summary["TP"] / (summary["TP"] + summary["FP"])).round(3)
    summary["recall"]    = (summary["TP"] / (summary["TP"] + summary["FN"])).round(3)

    return df, summary


# ══════════════════════════════════════════════════════════════════════════════
# 模組二：同 N 筆資料處理時間比較
# ══════════════════════════════════════════════════════════════════════════════

def analyze_module2(experiments: list[dict],
                    window_start_sec: int,
                    window_end_sec: int) -> pd.DataFrame:
    """
    對每個實驗，從 latency_data.csv 的 throughput 欄位做梯形積分，
    計算視窗 [window_start_sec, window_end_sec] 內 Source 消費的總筆數。

    接著對 baseline 和 propose 配對（相同 query + replayer_mode），
    用較少的那個（baseline）為基準 N，換算 propose 處理同 N 筆所需時間。

    回傳 DataFrame，每列為一個配對比較結果。
    """
    records = []

    for exp in experiments:
        latency_df = load_latency_csv(exp["latency_csv"])
        if latency_df.empty:
            continue

        # 取視窗內的資料
        window_df = latency_df[
            (latency_df['relative_time'] >= window_start_sec) &
            (latency_df['relative_time'] <= window_end_sec)
            ].copy()

        if window_df.empty or len(window_df) < 2:
            continue

        # 梯形積分：records_consumed = sum(throughput_i * delta_t_i)
        t    = window_df['relative_time'].values
        tps  = window_df['throughput'].values
        records_consumed = float(np.trapezoid(tps, t))

        # 實際視窗時間（秒）—— 取實際有資料的那段
        actual_duration_sec = float(t[-1] - t[0])

        records.append({
            "experiment_id":       exp["experiment_id"],
            "method":              exp["method"],
            "window_start_sec":    window_start_sec,
            "window_end_sec":      window_end_sec,
            "actual_duration_sec": round(actual_duration_sec, 1),
            "records_consumed":    int(records_consumed),
            "avg_tps":             round(records_consumed / actual_duration_sec, 1)
            if actual_duration_sec > 0 else 0.0,
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # ── 配對比較：baseline vs propose ─────────────────────────────────────────
    # 用實驗 ID 中去掉 method 前綴後的部分作為配對 key
    # 慣例：實驗 ID 格式為 "{method}_{query}_{mode}_{run_id}"
    def extract_pair_key(exp_id: str, method: str) -> str:
        key = exp_id.replace(f"{method}_", "", 1)
        # When exp_id has no suffix (e.g. just "baseline" or "propose"),
        # the replace above leaves the string unchanged — use "" as the shared pair key.
        return "" if key == exp_id else key

    df["pair_key"] = df.apply(
        lambda r: extract_pair_key(r["experiment_id"], r["method"]), axis=1
    )

    baseline_df = df[df["method"] == "baseline"].set_index("pair_key")
    propose_df  = df[df["method"] == "propose"].set_index("pair_key")

    comparison_rows = []
    for pair_key in baseline_df.index.intersection(propose_df.index):
        b = baseline_df.loc[pair_key]
        p = propose_df.loc[pair_key]

        N = int(b["records_consumed"])   # 用 baseline 的筆數為基準

        # propose 處理同 N 筆所需時間：
        #   propose 的 avg_tps 已知 → time_for_N = N / avg_tps
        p_time_for_N = N / p["avg_tps"] if p["avg_tps"] > 0 else float('inf')
        b_time_for_N = b["actual_duration_sec"]   # baseline 真實花的時間
        speedup      = b_time_for_N / p_time_for_N if p_time_for_N > 0 else float('inf')

        comparison_rows.append({
            "pair_key":             pair_key,
            "baseline_exp":         b["experiment_id"],
            "propose_exp":          p["experiment_id"],
            "N_records":            N,
            "baseline_time_sec":    round(b_time_for_N, 1),
            "propose_time_for_N_sec": round(p_time_for_N, 1),
            "speedup_ratio":        round(speedup, 3),
            "baseline_avg_tps":     b["avg_tps"],
            "propose_avg_tps":      p["avg_tps"],
        })

    comparison_df = pd.DataFrame(comparison_rows)
    return df, comparison_df


# ══════════════════════════════════════════════════════════════════════════════
# 模組三：遷移開銷統計
# ══════════════════════════════════════════════════════════════════════════════

def analyze_module3(experiments: list[dict]) -> pd.DataFrame:
    """
    從每個實驗的 migration_record.txt 解析遷移開銷，
    統計：
    - 每次遷移的中斷時間（total / savepoint / wait / restart）
    - 被遷移 subtask 的最大 state size 和總 state size
    - 每個 job 總共執行了幾次遷移

    回傳 (detail_df, summary_df)
    detail_df：每次遷移一列
    summary_df：每個實驗彙總（遷移次數、平均/最大中斷時間、state 統計）
    """
    detail_rows = []

    for exp in experiments:
        migrations = parse_migration_record(exp["migration_record"])
        experiment_id = exp["experiment_id"]
        method = exp["method"]

        for i, mig in enumerate(migrations, start=1):
            moved_subtasks = [s for s in mig["subtasks"] if s["actually_moved"]]
            state_sizes    = [s["state_size_bytes"] for s in moved_subtasks if s["state_size_bytes"] > 0]
            max_state      = max(state_sizes) if state_sizes else 0
            total_state    = sum(state_sizes)

            causes = list({s["bottleneck_cause"] for s in moved_subtasks})

            detail_rows.append({
                "experiment_id":        experiment_id,
                "method":               method,
                "migration_index":      i,
                "event_datetime":       mig["event_datetime"],
                "event_timestamp":      mig["event_timestamp"],
                "total_downtime_sec":   mig["total_downtime_sec"],
                "savepoint_sec":        mig["savepoint_sec"],
                "wait_sec":             mig["wait_sec"],
                "restart_sec":          mig["restart_sec"],
                "migrated_subtask_count": len(moved_subtasks),
                "max_state_size_bytes": max_state,
                "max_state_size_mb":    round(max_state / 1024**2, 2),
                "total_state_bytes":    total_state,
                "total_state_mb":       round(total_state / 1024**2, 2),
                "bottleneck_causes":    ";".join(causes),
                "job_id_before":        mig["job_id_before"],
                "job_id_after":         mig["job_id_after"],
            })

    detail_df = pd.DataFrame(detail_rows)
    if detail_df.empty:
        return detail_df, pd.DataFrame()

    # ── 彙總統計：每個實驗 ────────────────────────────────────────────────────
    summary_df = detail_df.groupby(["experiment_id", "method"]).agg(
        migration_count        = ("migration_index",      "max"),
        avg_downtime_sec       = ("total_downtime_sec",   "mean"),
        max_downtime_sec       = ("total_downtime_sec",   "max"),
        min_downtime_sec       = ("total_downtime_sec",   "min"),
        avg_savepoint_sec      = ("savepoint_sec",        "mean"),
        avg_restart_sec        = ("restart_sec",          "mean"),
        max_state_size_mb      = ("max_state_size_mb",    "max"),   # 所有遷移中最大的單一 state
        avg_total_state_mb     = ("total_state_mb",       "mean"),  # 平均每次遷移的 state 總和
    ).round(3).reset_index()

    return detail_df, summary_df


# ══════════════════════════════════════════════════════════════════════════════
# 繪圖
# ══════════════════════════════════════════════════════════════════════════════

def plot_module1(m1_df: pd.DataFrame, m1_summary: pd.DataFrame, fig_dir: str):
    """模組一：Precision / Recall 長條圖 + TP/FP/FN 分佈"""
    if m1_df.empty:
        return

    os.makedirs(fig_dir, exist_ok=True)

    # ── 圖1：各實驗 TP/FP/FN 分佈 ──
    fig, ax = plt.subplots(figsize=(max(8, len(m1_summary) * 1.5), 5))
    x = np.arange(len(m1_summary))
    w = 0.25
    ax.bar(x - w, m1_summary.get("TP", 0), w, label="TP", color="#2ca02c")
    ax.bar(x,     m1_summary.get("FP", 0), w, label="FP", color="#d62728")
    ax.bar(x + w, m1_summary.get("FN", 0), w, label="FN", color="#ff7f0e")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{eid}\n({mth})" for eid, mth in m1_summary.index],
        rotation=30, ha='right', fontsize=8
    )
    ax.set_ylabel("Count")
    ax.set_title("Module 1: Bottleneck Detection Results (TP / FP / FN)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m1_tp_fp_fn.png"), dpi=150)
    plt.close()

    # ── 圖2：Precision & Recall ──
    fig, ax = plt.subplots(figsize=(max(8, len(m1_summary) * 1.5), 5))
    ax.bar(x - 0.2, m1_summary.get("precision", 0), 0.4, label="Precision", color="#1f77b4")
    ax.bar(x + 0.2, m1_summary.get("recall",    0), 0.4, label="Recall",    color="#9467bd")
    ax.set_ylim(0, 1.1)
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{eid}\n({mth})" for eid, mth in m1_summary.index],
        rotation=30, ha='right', fontsize=8
    )
    ax.set_ylabel("Score")
    ax.set_title("Module 1: Bottleneck Detection Precision & Recall")
    ax.axhline(1.0, color='gray', linestyle='--', linewidth=0.8)
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m1_precision_recall.png"), dpi=150)
    plt.close()

    print(f"  ✅ 模組一圖表: {fig_dir}/m1_*.png")


def plot_module2(m2_raw: pd.DataFrame, m2_comparison: pd.DataFrame, fig_dir: str):
    """模組二：Speedup Ratio 長條圖 + baseline vs propose avg_tps 對比"""
    if m2_comparison.empty:
        return

    os.makedirs(fig_dir, exist_ok=True)

    # ── 圖1：Speedup Ratio ──
    fig, ax = plt.subplots(figsize=(max(6, len(m2_comparison) * 1.5), 5))
    colors = ["#2ca02c" if r >= 1.0 else "#d62728" for r in m2_comparison["speedup_ratio"]]
    ax.bar(range(len(m2_comparison)), m2_comparison["speedup_ratio"], color=colors)
    ax.axhline(1.0, color='black', linestyle='--', linewidth=1)
    ax.set_xticks(range(len(m2_comparison)))
    ax.set_xticklabels(m2_comparison["pair_key"], rotation=30, ha='right', fontsize=8)
    ax.set_ylabel("Speedup Ratio (Baseline Time / Propose Time)")
    ax.set_title("Module 2: Speedup Ratio for Processing N Records\n(>1 = Propose faster)")
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m2_speedup_ratio.png"), dpi=150)
    plt.close()

    # ── 圖2：baseline vs propose avg_tps 並排 ──
    fig, ax = plt.subplots(figsize=(max(6, len(m2_comparison) * 2), 5))
    x = np.arange(len(m2_comparison))
    w = 0.35
    ax.bar(x - w/2, m2_comparison["baseline_avg_tps"], w, label="Baseline", color="#aec7e8")
    ax.bar(x + w/2, m2_comparison["propose_avg_tps"],  w, label="Propose",  color="#1f77b4")
    ax.set_xticks(x)
    ax.set_xticklabels(m2_comparison["pair_key"], rotation=30, ha='right', fontsize=8)
    ax.set_ylabel("Avg Source TPS in Window (records/s)")
    ax.set_title("Module 2: Average Source Throughput in Analysis Window")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m2_avg_tps_comparison.png"), dpi=150)
    plt.close()

    print(f"  ✅ 模組二圖表: {fig_dir}/m2_*.png")


def plot_module3(m3_detail: pd.DataFrame, m3_summary: pd.DataFrame, fig_dir: str):
    """
    模組三：
    圖1 — baseline vs propose 每次遷移的中斷時間（散點 + 均值線）
    圖2 — 中斷時間分解：savepoint / wait / restart 堆疊長條
    圖3 — State Size vs 中斷時間散點圖
    """
    if m3_detail.empty:
        return

    os.makedirs(fig_dir, exist_ok=True)

    # ── 圖1：各方法的中斷時間分佈（box plot）──
    fig, ax = plt.subplots(figsize=(8, 5))
    methods = m3_detail["method"].unique()
    data_by_method = [
        m3_detail[m3_detail["method"] == m]["total_downtime_sec"].values
        for m in methods
    ]
    bp = ax.boxplot(data_by_method, tick_labels=methods, patch_artist=True,
                    medianprops=dict(color="black", linewidth=2))
    colors = {"baseline": "#aec7e8", "propose": "#1f77b4"}
    for patch, method in zip(bp['boxes'], methods):
        patch.set_facecolor(colors.get(method, "#cccccc"))
    ax.set_ylabel("Total Downtime (s)")
    ax.set_title("Module 3: Migration Downtime Distribution by Method")
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m3_downtime_boxplot.png"), dpi=150)
    plt.close()

    # ── 圖2：時間分解堆疊長條（每次遷移）──
    fig, ax = plt.subplots(figsize=(max(10, len(m3_detail) * 0.6), 5))
    x      = np.arange(len(m3_detail))
    labels = [f"{r['experiment_id']}\n#{r['migration_index']}"
              for _, r in m3_detail.iterrows()]

    ax.bar(x, m3_detail["savepoint_sec"], label="Savepoint",     color="#1f77b4")
    ax.bar(x, m3_detail["wait_sec"],      label="Resource Wait",  color="#ff7f0e",
           bottom=m3_detail["savepoint_sec"])
    ax.bar(x, m3_detail["restart_sec"],   label="Restart",        color="#2ca02c",
           bottom=m3_detail["savepoint_sec"] + m3_detail["wait_sec"])
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=7)
    ax.set_ylabel("Time (s)")
    ax.set_title("Module 3: Migration Time Breakdown (Savepoint / Wait / Restart)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(fig_dir, "m3_time_breakdown.png"), dpi=150)
    plt.close()

    # ── 圖3：State Size vs 中斷時間散點圖 ──
    has_state = m3_detail[m3_detail["total_state_mb"] > 0]
    if not has_state.empty:
        fig, ax = plt.subplots(figsize=(7, 5))
        method_colors = {"baseline": "#d62728", "propose": "#1f77b4"}
        for method, grp in has_state.groupby("method"):
            ax.scatter(grp["total_state_mb"], grp["total_downtime_sec"],
                       label=method, color=method_colors.get(method, "gray"),
                       alpha=0.7, s=60)
        ax.set_xlabel("Total State Size Migrated (MB)")
        ax.set_ylabel("Total Downtime (s)")
        ax.set_title("Module 3: State Size vs Migration Downtime")
        ax.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(fig_dir, "m3_state_vs_downtime.png"), dpi=150)
        plt.close()

    print(f"  ✅ 模組三圖表: {fig_dir}/m3_*.png")


def plot_latency_timelines(experiments: list[dict], m3_detail: pd.DataFrame, fig_dir: str):
    """
    附加圖：每個實驗的 latency 時間軸，並在遷移事件處標注垂直線。
    方便視覺化確認模組一的判斷是否合理。
    """
    os.makedirs(fig_dir, exist_ok=True)

    for exp in experiments:
        df = load_latency_csv(exp["latency_csv"])
        if df.empty:
            continue

        exp_id = exp["experiment_id"]
        method = exp["method"]
        exp_migrations = m3_detail[m3_detail["experiment_id"] == exp_id] if not m3_detail.empty else pd.DataFrame()

        exp_start_ts = df['unix_timestamp'].iloc[0]

        fig, ax = plt.subplots(figsize=(14, 5))
        ax.plot(df['relative_time'], df['total_latency_ms'],
                color="#1f77b4", linewidth=1.2, label="Latency")

        # 標注遷移事件
        if not exp_migrations.empty:
            for _, mrow in exp_migrations.iterrows():
                t_rel = mrow["event_timestamp"] - exp_start_ts
                t_done = t_rel + mrow["total_downtime_sec"]
                ax.axvspan(t_rel, t_done, alpha=0.2, color="red", label="_nolegend_")
                ax.axvline(t_done, color="red", linestyle="--", linewidth=1,
                           label=f"Migration #{mrow['migration_index']} done")

        ax.set_xlabel("Time (s)")
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"Latency Timeline: {exp_id}")
        handles, labels = ax.get_legend_handles_labels()
        unique = dict(zip(labels, handles))
        ax.legend(unique.values(), unique.keys(), fontsize=8)
        ax.grid(True, linestyle='--', alpha=0.4)
        plt.tight_layout()
        safe_name = exp_id.replace("/", "_")
        plt.savefig(os.path.join(fig_dir, f"latency_{safe_name}.png"), dpi=120)
        plt.close()

    print(f"  ✅ Latency 時間軸圖: {fig_dir}/latency_*.png")


# ══════════════════════════════════════════════════════════════════════════════
# 主程式
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Flink 遷移實驗結果分析")
    parser.add_argument("--output-root", default="./output",
                        help="實驗輸出根目錄（預設 ./output）")
    parser.add_argument("--replayer-log", default="./replayer.txt",
                        help="replayer.txt 路徑（用於決定分析視窗）")
    parser.add_argument("--mode", default=None,
                        choices=["scenario", "sine", "composite", "bursty", "random"],
                        help="replayer 模式（不指定時從 replayer.txt 自動偵測）")
    parser.add_argument("--result-dir", default="./analysis_results",
                        help="分析結果輸出目錄（預設 ./analysis_results）")
    args = parser.parse_args()

    os.makedirs(args.result_dir, exist_ok=True)
    fig_dir = os.path.join(args.result_dir, "figures")

    print("=" * 60)
    print("Flink 遷移實驗分析腳本")
    print("=" * 60)

    # ── 掃描實驗目錄 ──────────────────────────────────────────────────────────
    experiments = scan_experiments(args.output_root)
    if not experiments:
        print(f"❌ 在 {args.output_root} 下找不到任何含 latency_data.csv 的子目錄")
        return
    print(f"\n📁 找到 {len(experiments)} 個實驗：")
    for e in experiments:
        print(f"   [{e['method']:8s}] {e['experiment_id']}")

    # ── 解析 replayer 劇本 ────────────────────────────────────────────────────
    replayer_info = {}
    if os.path.exists(args.replayer_log):
        replayer_info = parse_replayer_log(args.replayer_log)
        tps_min = replayer_info.get('tps_min')
        tps_max = replayer_info.get('tps_max')
        tps_range_str = (f"TPS {tps_min:,} ~ {tps_max:,}"
                         if tps_min and tps_max else "TPS 範圍未知")
        print(f"\n📋 Replayer 劇本: {replayer_info.get('mode')} | {tps_range_str}")
        print(f"   timeline 取樣點數: {len(replayer_info.get('timeline', []))}")
    else:
        print(f"\n⚠️  找不到 {args.replayer_log}，視窗邊界將使用預設值")

    # ── 決定 effective_mode（手動指定優先，否則從 replayer.txt 自動偵測）──────
    detected_mode = replayer_info.get("mode", "unknown")
    # scenario 模式下 mode 值為劇本名稱（double_peak / triple_peak / step_stress）
    # 統一歸類為 "scenario"
    if detected_mode in ("double_peak", "triple_peak", "step_stress"):
        detected_mode = "scenario"

    if args.mode:
        effective_mode = args.mode
        print(f"⚙️  Replayer 模式: {effective_mode}（手動指定）")
    else:
        effective_mode = detected_mode if detected_mode != "unknown" else "scenario"
        source = "自動偵測" if detected_mode != "unknown" else "預設 fallback"
        print(f"⚙️  Replayer 模式: {effective_mode}（{source}）")

    # ── 決定分析視窗 ──────────────────────────────────────────────────────────
    window_start, window_end = get_window_bounds(replayer_info, effective_mode)
    print(f"⏱️  分析視窗: {window_start}s ~ {window_end}s")

    # ── 模組三（先跑，後面畫圖需要）────────────────────────────────────────
    print("\n" + "─" * 40)
    print("模組三：遷移開銷統計")
    m3_detail, m3_summary = analyze_module3(experiments)

    if not m3_detail.empty:
        m3_detail.to_csv(os.path.join(args.result_dir, "analysis_module3_overhead_detail.csv"), index=False)
        m3_summary.to_csv(os.path.join(args.result_dir, "analysis_module3_overhead_summary.csv"), index=False)
        print(m3_summary.to_string(index=False))
        plot_module3(m3_detail, m3_summary, fig_dir)
    else:
        print("⚠️  無遷移記錄可分析")

    # ── 模組一 ────────────────────────────────────────────────────────────────
    print("\n" + "─" * 40)
    print("模組一：瓶頸偵測準確率")
    result1 = analyze_module1(experiments)
    if isinstance(result1, tuple):
        m1_df, m1_summary = result1
        if not m1_df.empty:
            m1_df.to_csv(os.path.join(args.result_dir, "analysis_module1_bottleneck.csv"), index=False)
            m1_summary.to_csv(os.path.join(args.result_dir, "analysis_module1_summary.csv"))
            print(m1_summary.to_string())
            plot_module1(m1_df, m1_summary, fig_dir)
        else:
            print("⚠️  無資料可分析（latency CSV 可能為空）")
    else:
        print("⚠️  分析失敗")

    # ── 模組二 ────────────────────────────────────────────────────────────────
    print("\n" + "─" * 40)
    print("模組二：同 N 筆資料處理時間比較")
    result2 = analyze_module2(experiments, window_start, window_end)
    if isinstance(result2, tuple):
        m2_raw, m2_comparison = result2
        if not m2_raw.empty:
            m2_raw.to_csv(os.path.join(args.result_dir, "analysis_module2_throughput_raw.csv"), index=False)
        if not m2_comparison.empty:
            m2_comparison.to_csv(os.path.join(args.result_dir, "analysis_module2_comparison.csv"), index=False)
            print(m2_comparison[["pair_key", "N_records", "baseline_time_sec",
                                 "propose_time_for_N_sec", "speedup_ratio"]].to_string(index=False))
            plot_module2(m2_raw, m2_comparison, fig_dir)
        else:
            print("⚠️  無配對資料（需同時有 baseline 和 propose 實驗）")

    # ── 附加：Latency 時間軸圖 ────────────────────────────────────────────────
    print("\n" + "─" * 40)
    print("附加：繪製 Latency 時間軸圖（含遷移標注）")
    plot_latency_timelines(experiments, m3_detail, fig_dir)

    print("\n" + "=" * 60)
    print(f"✅ 分析完成，結果輸出至：{args.result_dir}/")
    print(f"   CSV：analysis_module1~3_*.csv")
    print(f"   圖表：figures/")
    print("=" * 60)


if __name__ == "__main__":
    main()