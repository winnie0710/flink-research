#!/usr/bin/env python3
"""
visualize_migration.py
======================
Animates the propose_v6.py bottleneck-diagnosis & migration algorithm.

Three panels
------------
  Left   : Logical DAG View          – operator-level health (colour-coded)
  Right  : Physical TM Deployment    – subtasks placed inside TM boxes;
                                       dashed purple arrow for migrations
  Bottom : Decision Process Log      – Diagnosis / Prioritization / Assignment

Usage
-----
  python visualize_migration.py            # uses built-in dummy data
  python visualize_migration.py --real     # reads real CSVs (new format)
"""

import os
import re
import sys
import argparse
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import networkx as nx
import pandas as pd
from collections import defaultdict

# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--real", action="store_true", help="Read real CSVs instead of dummy data")
args = parser.parse_args()
USE_REAL_CSV = args.real

# ── PATHS ─────────────────────────────────────────────────────────────────────
SUBTASK_CSV  = "/home/yenwei/research/structure_setup/output/t15_3/subtask_metrics_history.csv"
DETAIL_CSV   = "/home/yenwei/research/structure_setup/output/t15_3/migration_details.csv"
OUTPUT_FILE  = "/home/yenwei/research/structure_setup/output/t15_3/migration_animation.gif"
FRAME_MS     = 2000   # ms per frame

# ── STYLE ─────────────────────────────────────────────────────────────────────
BG_DARK   = "#1a1a2e"
BG_PANEL  = "#16213e"
BG_BOX    = "#0f3460"
ACCENT    = "#4a90d9"
TEXT_DIM  = "#7ec8e3"
TEXT_MAIN = "#e0e0e0"

STATUS_COLOR = {
    "Healthy":        "#2ecc71",
    "CPU_Bottleneck": "#e74c3c",
    "NET_Bottleneck": "#e67e22",
    "Migrating":      "#9b59b6",
    "unknown":        "#7f8c8d",
}
STATUS_PRIORITY = {"CPU_Bottleneck": 4, "NET_Bottleneck": 3, "Migrating": 2,
                   "Healthy": 1, "unknown": 0}

# ── DUMMY DATA (new CSV format) ───────────────────────────────────────────────
_D_SUB = [
    # t=1  all healthy
    (1,"Source_0","TM1","Healthy"), (1,"Source_1","TM2","Healthy"),
    (1,"Map_0","TM1","Healthy"),    (1,"Map_1","TM2","Healthy"),
    (1,"Join_0","TM3","Healthy"),   (1,"Join_1","TM4","Healthy"),
    (1,"Join_2","TM5","Healthy"),   (1,"Sink_0","TM5","Healthy"),
    # t=2  bottlenecks appear
    (2,"Source_0","TM1","Healthy"), (2,"Source_1","TM2","Healthy"),
    (2,"Map_0","TM1","Healthy"),    (2,"Map_1","TM2","NET_Bottleneck"),
    (2,"Join_0","TM3","Healthy"),   (2,"Join_1","TM4","CPU_Bottleneck"),
    (2,"Join_2","TM5","Healthy"),   (2,"Sink_0","TM5","Healthy"),
    # t=3  migration triggered
    (3,"Source_0","TM1","Healthy"), (3,"Source_1","TM2","Healthy"),
    (3,"Map_0","TM1","Healthy"),    (3,"Map_1","TM2","Migrating"),
    (3,"Join_0","TM3","Healthy"),   (3,"Join_1","TM4","Migrating"),
    (3,"Join_2","TM5","Healthy"),   (3,"Sink_0","TM5","Healthy"),
    # t=4  post-migration
    (4,"Source_0","TM1","Healthy"), (4,"Source_1","TM2","Healthy"),
    (4,"Map_0","TM1","Healthy"),    (4,"Map_1","TM1","Healthy"),
    (4,"Join_0","TM3","Healthy"),   (4,"Join_1","TM1","Healthy"),
    (4,"Join_2","TM5","Healthy"),   (4,"Sink_0","TM5","Healthy"),
]
DUMMY_SUBTASK = pd.DataFrame(_D_SUB,
    columns=["timestamp","subtask_id","current_tm","status"])

DUMMY_DETAIL = pd.DataFrame([
    (3,"Diagnosis",      "Join_1", "",  "TM4","",    "Detected CPU Overload"),
    (3,"Diagnosis",      "Map_1",  "",  "TM2","",    "Detected NET Overload"),
    (3,"Prioritization", "Join_1",  1,  "TM4","",    "Priority=0.923 (D_overload, Rank 1)"),
    (3,"Prioritization", "Map_1",   2,  "TM2","",    "Priority=0.741 (D_overload(net), Rank 2)"),
    (3,"Assignment",     "Join_1",  1,  "TM4","TM1", "TM1 selected due to max absolute CPUscore"),
    (3,"Assignment",     "Map_1",   2,  "TM2","TM1", "TM1 selected due to network topology affinity"),
], columns=["timestamp","decision_step","subtask_id","priority_rank","from_tm","to_tm","decision_reason"])

# Dummy TM config  (name → display_label)
DUMMY_TM_LABEL = {
    "TM1": "TM1\n(400c)", "TM2": "TM2\n(400c)",
    "TM3": "TM3\n(200c)", "TM4": "TM4\n(200c)",
    "TM5": "TM5\n(200c)",
}
DUMMY_DAG_OPS   = ["Source","Map","Join","Sink"]
DUMMY_DAG_EDGES = [("Source","Map"),("Map","Join"),("Join","Sink")]

# ── REAL DATA CONFIG ──────────────────────────────────────────────────────────
# Operator detection rules for real NexMark subtask IDs (order matters)
_OP_RULES = [
    (r"KafkaSource|Filter_Bids|Map_To_Bid", "Source"),
    (r"Window_Max",                          "WindowMax"),
    (r"Window_Join",                         "Join"),
    (r"KafkaSink|Sink:",                     "Sink"),
]

def detect_operator(subtask_id: str) -> str:
    for pattern, label in _OP_RULES:
        if re.search(pattern, subtask_id):
            return label
    # Fallback: strip trailing _N and return first token
    base = re.sub(r"_\d+$", "", subtask_id)
    return base.split("_")[0]

def short_label(subtask_id: str) -> str:
    """Two-char display label: op initial + subtask index."""
    op  = detect_operator(subtask_id)
    idx = re.search(r"_(\d+)$", subtask_id)
    num = idx.group(1) if idx else "0"
    return f"{op[0]}{num}"

REAL_DAG_OPS   = ["Source","WindowMax","Join","Sink"]
REAL_DAG_EDGES = [("Source","WindowMax"),("WindowMax","Join"),("Join","Sink")]

def tm_display_label(tm_id: str) -> str:
    m = re.search(r"(\d+c)", tm_id)
    cap = m.group(1) if m else ""
    idx = re.search(r"_(\d+)$", tm_id)
    num = idx.group(1) if idx else tm_id
    return f"TM{num}\n({cap})" if cap else f"TM{num}"

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
def load_data():
    if USE_REAL_CSV and os.path.exists(SUBTASK_CSV) and os.path.exists(DETAIL_CSV):
        df_s = pd.read_csv(SUBTASK_CSV)
        df_d = pd.read_csv(DETAIL_CSV)
        # Bin epoch timestamps to integers for frame sequencing
        t0 = df_s["timestamp"].min()
        df_s["timestamp"] = ((df_s["timestamp"] - t0)).astype(int)
        df_d["timestamp"] = ((df_d["timestamp"] - t0)).astype(int)
        dag_ops   = REAL_DAG_OPS
        dag_edges = REAL_DAG_EDGES
        # Build display label map for real subtask_ids
        all_sids  = df_s["subtask_id"].unique()
        sid_label = {s: short_label(s) for s in all_sids}
        sid_op    = {s: detect_operator(s) for s in all_sids}
        # Build TM label map
        all_tms   = df_s["current_tm"].unique()
        tm_label  = {t: tm_display_label(t) for t in all_tms}
    else:
        df_s      = DUMMY_SUBTASK.copy()
        df_d      = DUMMY_DETAIL.copy()
        dag_ops   = DUMMY_DAG_OPS
        dag_edges = DUMMY_DAG_EDGES
        all_sids  = df_s["subtask_id"].unique()
        sid_label = {s: s.split("_")[-1] for s in all_sids}   # "Join_1" → "1"
        sid_op    = {s: s.rsplit("_",1)[0] for s in all_sids}  # "Join_1" → "Join"
        all_tms   = list(DUMMY_TM_LABEL.keys())
        tm_label  = DUMMY_TM_LABEL

    timestamps = sorted(df_s["timestamp"].unique())
    return df_s, df_d, dag_ops, dag_edges, sid_label, sid_op, tm_label, timestamps

df_subtask, df_detail, DAG_OPS, DAG_EDGES, SID_LABEL, SID_OP, TM_LABEL, TIMESTAMPS = load_data()

# ── DAG GRAPH ─────────────────────────────────────────────────────────────────
G_dag = nx.DiGraph()
G_dag.add_nodes_from(DAG_OPS)
G_dag.add_edges_from(DAG_EDGES)
N = len(DAG_OPS)
DAG_POS = {op: (i, 0) for i, op in enumerate(DAG_OPS)}

# ── TM LAYOUT ─────────────────────────────────────────────────────────────────
ALL_TMS = list(TM_LABEL.keys())

def build_tm_positions(tms):
    """Auto-arrange TMs in a two-row grid."""
    cols = 3 if len(tms) >= 4 else len(tms)
    pos  = {}
    for i, tm in enumerate(tms):
        r = i // cols
        c = i % cols
        pos[tm] = (c * 2.2, -r * 2.2)
    return pos

TM_POS = build_tm_positions(ALL_TMS)
TM_W, TM_H = 1.8, 1.6

def subtask_offsets(n: int):
    """(dx, dy) offsets for n subtasks inside a TM box."""
    cols = min(n, 3)
    rows = (n + cols - 1) // cols
    pts  = []
    for i in range(n):
        r = i // cols
        c = i % cols
        pts.append(((c - (cols-1)/2) * 0.45, (r - (rows-1)/2) * 0.38))
    return pts

# ── FIGURE ────────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(20, 11), facecolor=BG_DARK)
gs  = GridSpec(2, 2, figure=fig,
               height_ratios=[3.2, 1.4],
               hspace=0.28, wspace=0.12,
               left=0.04, right=0.97, top=0.94, bottom=0.03)

ax_dag = fig.add_subplot(gs[0, 0])
ax_tm  = fig.add_subplot(gs[0, 1])
ax_log = fig.add_subplot(gs[1, :])

for ax in (ax_dag, ax_tm, ax_log):
    ax.set_facecolor(BG_PANEL)
    for sp in ax.spines.values():
        sp.set_edgecolor(BG_BOX)

# Legend
legend_patches = [
    mpatches.Patch(color=STATUS_COLOR["Healthy"],        label="Healthy"),
    mpatches.Patch(color=STATUS_COLOR["CPU_Bottleneck"], label="CPU Bottleneck"),
    mpatches.Patch(color=STATUS_COLOR["NET_Bottleneck"], label="NET Bottleneck"),
    mpatches.Patch(color=STATUS_COLOR["Migrating"],      label="Migrating"),
]
fig.legend(handles=legend_patches,
           loc="upper right", ncol=4,
           framealpha=0.25, facecolor=BG_DARK, edgecolor=ACCENT,
           labelcolor=TEXT_MAIN, fontsize=9)

# ── DRAW HELPERS ──────────────────────────────────────────────────────────────
def draw_dag(ax, subtask_stat, ts):
    ax.cla()
    ax.set_facecolor(BG_PANEL)
    ax.set_title(f"Logical DAG View   [t = {ts}]",
                 color=TEXT_MAIN, fontsize=13, pad=8)

    # Group subtasks by operator (in DAG order)
    op_subtasks: dict[str, list] = {op: [] for op in DAG_OPS}
    for sid in sorted(subtask_stat.keys()):          # stable sort for layout
        op = SID_OP.get(sid, "unknown")
        if op in op_subtasks:
            op_subtasks[op].append(sid)

    # ── Layout: each operator = one column, subtasks stacked vertically ────────
    COL_GAP  = 3.0    # horizontal gap between operator columns
    NODE_R   = 0.26   # node radius
    ROW_GAP  = 0.75   # vertical gap between subtasks in same column

    sid_pos: dict[str, tuple] = {}
    for col_idx, op in enumerate(DAG_OPS):
        sids = op_subtasks[op]
        n    = len(sids)
        x    = col_idx * COL_GAP
        for row_idx, sid in enumerate(sids):
            y = (n - 1) / 2.0 * ROW_GAP - row_idx * ROW_GAP
            sid_pos[sid] = (x, y)

    # ── Operator group background boxes ───────────────────────────────────────
    for col_idx, op in enumerate(DAG_OPS):
        sids = op_subtasks[op]
        if not sids:
            continue
        xs = [sid_pos[s][0] for s in sids]
        ys = [sid_pos[s][1] for s in sids]
        pad = 0.45
        rect = mpatches.FancyBboxPatch(
            (min(xs) - pad, min(ys) - pad),
            (max(xs) - min(xs)) + pad * 2,
            (max(ys) - min(ys)) + pad * 2,
            boxstyle="round,pad=0.05", linewidth=1.2,
            edgecolor=ACCENT, facecolor=BG_BOX, alpha=0.35, zorder=0
        )
        ax.add_patch(rect)
        # Operator label above the box
        ax.text(min(xs) + (max(xs) - min(xs)) / 2,
                max(ys) + pad + 0.12, op,
                ha="center", va="bottom",
                color=TEXT_DIM, fontsize=9.5, fontweight="bold")

    # ── Edges between adjacent operators (all-to-all, low alpha) ──────────────
    for (op_a, op_b) in DAG_EDGES:
        sids_a = op_subtasks.get(op_a, [])
        sids_b = op_subtasks.get(op_b, [])
        for sa in sids_a:
            for sb in sids_b:
                xa, ya = sid_pos[sa]
                xb, yb = sid_pos[sb]
                ax.annotate(
                    "", xy=(xb - NODE_R, yb), xytext=(xa + NODE_R, ya),
                    arrowprops=dict(
                        arrowstyle="-|>", color=ACCENT,
                        lw=0.9, alpha=0.35,
                        connectionstyle="arc3,rad=0.0"
                    ), zorder=1
                )

    # ── Subtask nodes ──────────────────────────────────────────────────────────
    for sid, (x, y) in sid_pos.items():
        color = STATUS_COLOR.get(subtask_stat.get(sid, "unknown"), "#7f8c8d")
        # Outer glow ring for bottleneck/migrating states
        stat = subtask_stat.get(sid, "unknown")
        if stat in ("CPU_Bottleneck", "NET_Bottleneck", "Migrating"):
            glow = plt.Circle((x, y), NODE_R + 0.08,
                               color=color, alpha=0.25, zorder=2)
            ax.add_patch(glow)
        circ = plt.Circle((x, y), NODE_R, color=color, zorder=3)
        ax.add_patch(circ)
        lbl = SID_LABEL.get(sid, sid[-2:])
        ax.text(x, y, lbl, ha="center", va="center",
                color="white", fontsize=8, fontweight="bold", zorder=4)

    # ── Axis limits ───────────────────────────────────────────────────────────
    if sid_pos:
        all_x = [p[0] for p in sid_pos.values()]
        all_y = [p[1] for p in sid_pos.values()]
        ax.set_xlim(min(all_x) - 1.2, max(all_x) + 1.2)
        ax.set_ylim(min(all_y) - 1.0, max(all_y) + 1.2)
    ax.axis("off")


def draw_tm(ax, subtask_tm, subtask_stat, migration_arrows, ts):
    ax.cla()
    ax.set_facecolor(BG_PANEL)
    ax.set_title(f"Physical TM Deployment   [t = {ts}]",
                 color=TEXT_MAIN, fontsize=13, pad=8)

    # TM boxes
    for tm in ALL_TMS:
        if tm not in TM_POS:
            continue
        cx, cy = TM_POS[tm]
        rect = mpatches.FancyBboxPatch(
            (cx - TM_W/2, cy - TM_H/2), TM_W, TM_H,
            boxstyle="round,pad=0.06", linewidth=1.8,
            edgecolor=ACCENT, facecolor=BG_BOX, alpha=0.9, zorder=1
        )
        ax.add_patch(rect)
        ax.text(cx, cy + TM_H/2 + 0.10, TM_LABEL.get(tm, tm),
                ha="center", va="bottom",
                color=TEXT_DIM, fontsize=8.5, fontweight="bold")

    # Subtask nodes inside TMs
    tm_buckets: dict[str, list] = defaultdict(list)
    for sid, tm in subtask_tm.items():
        tm_buckets[tm].append(sid)

    sid_plot_pos: dict[str, tuple] = {}
    for tm, sids in tm_buckets.items():
        if tm not in TM_POS:
            continue
        cx, cy = TM_POS[tm]
        for sid, (dx, dy) in zip(sids, subtask_offsets(len(sids))):
            sx, sy = cx + dx, cy + dy
            sid_plot_pos[sid] = (sx, sy)
            color = STATUS_COLOR.get(subtask_stat.get(sid, "unknown"), "#7f8c8d")
            circ = plt.Circle((sx, sy), 0.16, color=color, zorder=4)
            ax.add_patch(circ)
            lbl = SID_LABEL.get(sid, sid[-2:])
            ax.text(sx, sy, lbl, ha="center", va="center",
                    color="white", fontsize=7, fontweight="bold", zorder=5)

    # Migration dashed arrows
    for from_tm, to_tm in migration_arrows:
        if from_tm not in TM_POS or to_tm not in TM_POS or from_tm == to_tm:
            continue
        fx, fy = TM_POS[from_tm]
        tx, ty = TM_POS[to_tm]
        ax.annotate(
            "", xy=(tx, ty), xytext=(fx, fy),
            arrowprops=dict(
                arrowstyle="-|>", color="#9b59b6", lw=2.2,
                linestyle="dashed",
                connectionstyle="arc3,rad=0.25"
            ), zorder=8
        )

    # Auto limits
    all_x = [p[0] for p in TM_POS.values()]
    all_y = [p[1] for p in TM_POS.values()]
    ax.set_xlim(min(all_x) - TM_W, max(all_x) + TM_W)
    ax.set_ylim(min(all_y) - TM_H * 1.5, max(all_y) + TM_H * 1.5)
    ax.axis("off")


def draw_log(ax, det_ts, ts):
    ax.cla()
    ax.set_facecolor(BG_PANEL)
    ax.axis("off")

    if det_ts.empty:
        text = f"[t={ts}]  No migration triggered — system nominal."
    else:
        lines = [f"[t = {ts}]  Migration Decision Log"]
        lines.append("─" * 80)

        diag = det_ts[det_ts["decision_step"] == "Diagnosis"]
        if not diag.empty:
            lines.append("① Diagnosis:")
            for _, r in diag.iterrows():
                lines.append(f"   • {r['subtask_id']}  @  {r['from_tm']}  →  {r['decision_reason']}")

        prio = det_ts[det_ts["decision_step"] == "Prioritization"]
        if not prio.empty:
            lines.append("② Prioritization (sorted by Priority Score):")
            for _, r in prio.sort_values("priority_rank").iterrows():
                lines.append(f"   • Rank {r['priority_rank']}: {r['subtask_id']}    {r['decision_reason']}")

        asgn = det_ts[det_ts["decision_step"] == "Assignment"]
        if not asgn.empty:
            lines.append("③ Target Assignment:")
            for _, r in asgn.iterrows():
                lines.append(f"   • {r['subtask_id']}    {r['from_tm']}  ──▶  {r['to_tm']}    [{r['decision_reason']}]")

        text = "\n".join(lines)

    ax.text(
        0.01, 0.97, text,
        transform=ax.transAxes,
        va="top", ha="left",
        color=TEXT_MAIN, fontsize=9,
        fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.5",
                  facecolor=BG_BOX, edgecolor=ACCENT, alpha=0.92),
    )

# ── ANIMATION FRAME ───────────────────────────────────────────────────────────
def update(frame):
    ts = TIMESTAMPS[frame]

    sub_ts = df_subtask[df_subtask["timestamp"] == ts]
    subtask_tm   = dict(zip(sub_ts["subtask_id"], sub_ts["current_tm"]))
    subtask_stat = dict(zip(sub_ts["subtask_id"], sub_ts["status"]))

    det_ts = df_detail[df_detail["timestamp"] == ts]

    # Collect migration arrows from Assignment rows
    migration_arrows = []
    for _, r in det_ts[det_ts["decision_step"] == "Assignment"].iterrows():
        ft = str(r["from_tm"]); tt = str(r["to_tm"])
        if ft and tt and ft != tt:
            migration_arrows.append((ft, tt))

    draw_dag(ax_dag, subtask_stat, ts)
    draw_tm(ax_tm, subtask_tm, subtask_stat, migration_arrows, ts)
    draw_log(ax_log, det_ts, ts)
    return []

# ── RUN ───────────────────────────────────────────────────────────────────────
ani = animation.FuncAnimation(
    fig, update,
    frames=len(TIMESTAMPS),
    interval=FRAME_MS,
    blit=False,
    repeat=True,
)

os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
ani.save(OUTPUT_FILE, writer="pillow", fps=1, dpi=130)
print(f"✅  Saved → {OUTPUT_FILE}")
