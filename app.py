"""
IMSS Batch Management & Allocation Module
==========================================
Single-file Streamlit prototype using in-memory pandas DataFrames.

TO SWAP IN A DATABASE LATER:
  1. Replace `init_data()` with DB read functions (SQLAlchemy / psycopg2 etc.)
  2. Replace every `st.session_state.<table>` write with DB INSERT/UPDATE calls.
  3. Remove session_state persistence for tables; keep it only for UI state.
  4. The helper functions (allocation_engine, derive_line_status, etc.) stay unchanged.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
import uuid
import os


# ─────────────────────────────────────────────────────────────
# LOAD BRIGADES FROM CSV
# ─────────────────────────────────────────────────────────────
def load_brigades() -> list:
    """
    Load brigade names from Brigades_20260226.csv if present alongside app.py.
    Falls back to demo names if file not found.
    TO REPLACE WITH DB: query the Units/Brigades table directly.
    """
    csv_path = os.path.join(os.path.dirname(__file__), "Brigades_20260226.csv")
    if not os.path.exists(csv_path):
        # fallback
        return ["1st Brigade", "2nd Brigade"]
    df = pd.read_csv(csv_path, comment="#")
    df.columns = [c.strip() for c in df.columns]
    # Use "Code - NameEn" as the display label so it's meaningful
    brigades = (df["Code"].str.strip() + " — " + df["NameEn"].str.strip()).tolist()
    return sorted(brigades)

BRIGADES = load_brigades()

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IMSS Batch Management",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# CUSTOM STYLES
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}
code, .stCode { font-family: 'IBM Plex Mono', monospace; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0f1117 !important;
    border-right: 1px solid #2a2d3a;
}
[data-testid="stSidebar"] * { color: #e0e4f0 !important; }
[data-testid="stSidebar"] .stRadio label { 
    padding: 6px 10px; border-radius: 4px; cursor: pointer;
}
[data-testid="stSidebar"] .stRadio label:hover { background: #1e2130; }

/* KPI Cards */
.kpi-card {
    background: #1a1d2e;
    border: 1px solid #2d3150;
    border-left: 4px solid #4f8ef7;
    border-radius: 6px;
    padding: 16px 20px;
    margin-bottom: 8px;
}
.kpi-value { font-size: 2rem; font-weight: 700; color: #4f8ef7; font-family: 'IBM Plex Mono'; }
.kpi-label { font-size: 0.78rem; color: #7880a0; text-transform: uppercase; letter-spacing: 0.08em; }

/* Status pills */
.pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.pill-waiting   { background:#2d1f3d; color:#c084fc; }
.pill-partial   { background:#2d2012; color:#fb923c; }
.pill-ready     { background:#0f2d1f; color:#4ade80; }
.pill-critical  { background:#3d0f0f; color:#f87171; }
.pill-high      { background:#2d1f0f; color:#fbbf24; }
.pill-normal    { background:#1a2030; color:#94a3b8; }

/* Section headers */
.section-header {
    font-size: 1.3rem; font-weight: 700;
    color: #e0e4f0;
    border-bottom: 2px solid #2d3150;
    padding-bottom: 8px; margin-bottom: 16px;
    font-family: 'IBM Plex Mono';
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# ID GENERATORS
# ─────────────────────────────────────────────────────────────
def next_id(prefix: str, existing_ids: list) -> str:
    nums = []
    for eid in existing_ids:
        try:
            nums.append(int(str(eid).replace(prefix + "-", "")))
        except ValueError:
            pass
    nxt = (max(nums) + 1) if nums else 1
    return f"{prefix}-{nxt:04d}"


# ─────────────────────────────────────────────────────────────
# DERIVED FIELDS
# ─────────────────────────────────────────────────────────────
def derive_wo_part_lines(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["outstanding_qty"] = (df["required_qty"] - df["received_qty"]).clip(lower=0)
    df["line_status"] = df.apply(
        lambda r: "Ready" if r["received_qty"] >= r["required_qty"]
        else ("Partial" if r["received_qty"] > 0 else "Waiting"),
        axis=1,
    )
    return df


def derive_batch_status_from_lines(batch_id: str) -> str:
    """Recalculate batch status from its batch lines."""
    lines = st.session_state.batch_lines[
        st.session_state.batch_lines["batch_id"] == batch_id
    ]
    if lines.empty:
        return "Draft"
    total_req = lines["total_required_qty"].sum()
    total_rec = lines["received_qty"].sum()
    if total_rec == 0:
        return "Under Procurement"
    if total_rec >= total_req:
        return "Fully Received"
    return "Partially Received"


# ─────────────────────────────────────────────────────────────
# ALLOCATION ENGINE
# ─────────────────────────────────────────────────────────────
PRIORITY_ORDER = {"Critical": 0, "High": 1, "Normal": 2}


def run_allocation_engine(batch_line_id: str):
    """
    Distribute batch line received_qty across linked WO part lines.
    Rule: Critical first, then High, then Normal; within same priority use FIFO (created_date ASC).
    Updates: Allocations.allocated_qty, WorkOrderPartLines.received_qty
    """
    bl = st.session_state.batch_lines[
        st.session_state.batch_lines["batch_line_id"] == batch_line_id
    ]
    if bl.empty:
        return
    batch_received = int(bl.iloc[0]["received_qty"])

    allocs = st.session_state.allocations[
        st.session_state.allocations["batch_line_id"] == batch_line_id
    ].copy()
    if allocs.empty:
        return

    # Merge WO info for ordering
    wo_df = st.session_state.work_orders[["wo_id", "priority", "created_date"]]
    allocs = allocs.merge(wo_df, on="wo_id", how="left")
    # Merge required_qty from WO part lines
    wpl = st.session_state.wo_part_lines[["line_id", "required_qty"]]
    allocs = allocs.merge(wpl, on="line_id", how="left")

    allocs["priority_rank"] = allocs["priority"].map(PRIORITY_ORDER).fillna(9)
    allocs = allocs.sort_values(["priority_rank", "created_date"]).reset_index(drop=True)

    remaining = batch_received
    for idx, row in allocs.iterrows():
        line_rec = int(
            st.session_state.wo_part_lines.loc[
                st.session_state.wo_part_lines["line_id"] == row["line_id"], "received_qty"
            ].values[0]
        )
        req = int(row["required_qty"])
        outstanding = max(0, req - line_rec)
        give = min(outstanding, remaining)
        allocs.at[idx, "allocated_qty"] = give
        remaining -= give

    # Write back allocated_qty to Allocations
    for _, row in allocs.iterrows():
        mask = st.session_state.allocations["allocation_id"] == row["allocation_id"]
        st.session_state.allocations.loc[mask, "allocated_qty"] = row["allocated_qty"]
        st.session_state.allocations.loc[mask, "last_updated"] = datetime.now()

    # Update WO part lines received_qty from sum of allocations per line
    for _, row in allocs.iterrows():
        line_allocs = st.session_state.allocations[
            st.session_state.allocations["line_id"] == row["line_id"]
        ]["allocated_qty"].sum()
        st.session_state.wo_part_lines.loc[
            st.session_state.wo_part_lines["line_id"] == row["line_id"], "received_qty"
        ] = int(line_allocs)

    st.session_state.wo_part_lines = derive_wo_part_lines(st.session_state.wo_part_lines)


# ─────────────────────────────────────────────────────────────
# SAMPLE DATA INITIALISER
# ─────────────────────────────────────────────────────────────
def init_data():
    today = date.today()

    # ── Work Orders ──────────────────────────────────────────
    # Use real brigade codes from CSV (first two for demo)
    b1 = "KAMB — King Abdulaziz Mechanized Brigade"
    b2 = "IMSMB — Imam Muhammad bin Saud Mechanized Brigade"

    work_orders = pd.DataFrame([
        {"wo_id": "WO-0001", "brigade": b1, "workshop": "Workshop Alpha",
         "created_date": today - timedelta(days=30), "priority": "Critical",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0002", "brigade": b1, "workshop": "Workshop Alpha",
         "created_date": today - timedelta(days=25), "priority": "High",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0003", "brigade": b1, "workshop": "Workshop Bravo",
         "created_date": today - timedelta(days=22), "priority": "Normal",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0004", "brigade": b1, "workshop": "Workshop Bravo",
         "created_date": today - timedelta(days=18), "priority": "High",
         "status": "Under Maintenance"},
        {"wo_id": "WO-0005", "brigade": b1, "workshop": "Workshop Bravo",
         "created_date": today - timedelta(days=15), "priority": "Critical",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0006", "brigade": b1, "workshop": "Workshop Charlie",
         "created_date": today - timedelta(days=10), "priority": "Normal",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0007", "brigade": b2, "workshop": "Workshop Alpha",
         "created_date": today - timedelta(days=28), "priority": "Critical",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0008", "brigade": b2, "workshop": "Workshop Alpha",
         "created_date": today - timedelta(days=20), "priority": "High",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0009", "brigade": b2, "workshop": "Workshop Bravo",
         "created_date": today - timedelta(days=14), "priority": "Normal",
         "status": "Waiting Parts"},
        {"wo_id": "WO-0010", "brigade": b2, "workshop": "Workshop Charlie",
         "created_date": today - timedelta(days=7),  "priority": "High",
         "status": "Closed"},
    ])

    # ── WO Part Lines (same part_no across multiple WOs to demo batch aggregation) ──
    # PART-001 (engine filter) needed by WO-0001,0002,0003,0005,0007,0009 — total ~100 units
    # PART-002 (brake pad) across several WOs
    # PART-003 (hydraulic hose) unique to a few
    wo_part_lines = pd.DataFrame([
        # WO-0001 (Critical)
        {"line_id": "LN-0001", "wo_id": "WO-0001", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 20, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0002", "wo_id": "WO-0001", "part_no": "PART-002",
         "part_desc": "Brake Pad Set", "required_qty": 8, "allocated_qty": 0, "received_qty": 0},
        # WO-0002 (High)
        {"line_id": "LN-0003", "wo_id": "WO-0002", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 15, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0004", "wo_id": "WO-0002", "part_no": "PART-003",
         "part_desc": "Hydraulic Hose 1/2\"", "required_qty": 5, "allocated_qty": 0, "received_qty": 0},
        # WO-0003 (Normal)
        {"line_id": "LN-0005", "wo_id": "WO-0003", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 12, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0006", "wo_id": "WO-0003", "part_no": "PART-004",
         "part_desc": "Air Filter Element", "required_qty": 10, "allocated_qty": 0, "received_qty": 0},
        # WO-0004 (High - Under Maintenance, existing batch)
        {"line_id": "LN-0007", "wo_id": "WO-0004", "part_no": "PART-002",
         "part_desc": "Brake Pad Set", "required_qty": 4, "allocated_qty": 0, "received_qty": 0},
        # WO-0005 (Critical)
        {"line_id": "LN-0008", "wo_id": "WO-0005", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 18, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0009", "wo_id": "WO-0005", "part_no": "PART-005",
         "part_desc": "Coolant Tank Cap", "required_qty": 3, "allocated_qty": 0, "received_qty": 0},
        # WO-0006 (Normal)
        {"line_id": "LN-0010", "wo_id": "WO-0006", "part_no": "PART-003",
         "part_desc": "Hydraulic Hose 1/2\"", "required_qty": 8, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0011", "wo_id": "WO-0006", "part_no": "PART-004",
         "part_desc": "Air Filter Element", "required_qty": 6, "allocated_qty": 0, "received_qty": 0},
        # WO-0007 (Critical, 2nd Brigade)
        {"line_id": "LN-0012", "wo_id": "WO-0007", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 20, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0013", "wo_id": "WO-0007", "part_no": "PART-006",
         "part_desc": "Drive Belt", "required_qty": 4, "allocated_qty": 0, "received_qty": 0},
        # WO-0008 (High, 2nd Brigade)
        {"line_id": "LN-0014", "wo_id": "WO-0008", "part_no": "PART-002",
         "part_desc": "Brake Pad Set", "required_qty": 8, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0015", "wo_id": "WO-0008", "part_no": "PART-006",
         "part_desc": "Drive Belt", "required_qty": 2, "allocated_qty": 0, "received_qty": 0},
        # WO-0009 (Normal, 2nd Brigade)
        {"line_id": "LN-0016", "wo_id": "WO-0009", "part_no": "PART-001",
         "part_desc": "Engine Oil Filter", "required_qty": 15, "allocated_qty": 0, "received_qty": 0},
        {"line_id": "LN-0017", "wo_id": "WO-0009", "part_no": "PART-005",
         "part_desc": "Coolant Tank Cap", "required_qty": 5, "allocated_qty": 0, "received_qty": 0},
        # WO-0010 (Closed, no procurement needed but kept for reference)
        {"line_id": "LN-0018", "wo_id": "WO-0010", "part_no": "PART-004",
         "part_desc": "Air Filter Element", "required_qty": 8, "allocated_qty": 0, "received_qty": 8},
    ])

    wo_part_lines = derive_wo_part_lines(wo_part_lines)

    # ── Empty tables ──────────────────────────────────────────
    batches = pd.DataFrame(columns=[
        "batch_id", "brigade", "created_by", "created_date",
        "approval_ref", "batch_status",
    ])
    batch_lines = pd.DataFrame(columns=[
        "batch_line_id", "batch_id", "part_no", "part_desc",
        "total_required_qty", "vendor", "po_numbers",
        "ordered_qty", "received_qty", "expected_delivery_date",
    ])
    allocations = pd.DataFrame(columns=[
        "allocation_id", "batch_line_id", "wo_id", "line_id",
        "allocated_qty", "allocation_status", "last_updated", "notes",
    ])

    st.session_state.work_orders = work_orders
    st.session_state.wo_part_lines = wo_part_lines
    st.session_state.batches = batches
    st.session_state.batch_lines = batch_lines
    st.session_state.allocations = allocations


def maybe_init():
    if "work_orders" not in st.session_state:
        init_data()


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
def sidebar():
    with st.sidebar:
        st.markdown("## 🔧 IMSS Batch Mgmt")
        st.markdown("---")
        page = st.radio(
            "Navigation",
            ["📋 Work Orders", "➕ Create Batch", "📦 Procurement Updates",
             "🗂️ Allocation & Packing", "📊 Dashboards"],
            label_visibility="collapsed",
        )
        st.markdown("---")
        if st.button("🔄 Reset Demo Data", use_container_width=True):
            for key in ["work_orders", "wo_part_lines", "batches", "batch_lines", "allocations"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.success("Demo data reset.")
            st.rerun()
    return page


# ─────────────────────────────────────────────────────────────
# PAGE 1 — WORK ORDERS
# ─────────────────────────────────────────────────────────────
def page_work_orders():
    st.markdown('<div class="section-header">📋 Work Orders</div>', unsafe_allow_html=True)

    wo = st.session_state.work_orders.copy()
    wpl = st.session_state.wo_part_lines.copy()

    c1, c2, c3, c4 = st.columns(4)
    brigades = ["All"] + BRIGADES
    workshops = ["All"] + sorted(wo["workshop"].unique().tolist())
    statuses = ["All"] + sorted(wo["status"].unique().tolist())
    priorities = ["All"] + ["Critical", "High", "Normal"]

    with c1:
        f_brigade = st.selectbox("Brigade", brigades)
    with c2:
        f_workshop = st.selectbox("Workshop", workshops)
    with c3:
        f_status = st.selectbox("Status", statuses)
    with c4:
        f_priority = st.selectbox("Priority", priorities)

    dr_col1, dr_col2 = st.columns(2)
    with dr_col1:
        f_date_from = st.date_input("Created From", value=date.today() - timedelta(days=90))
    with dr_col2:
        f_date_to = st.date_input("Created To", value=date.today())

    # Apply filters
    mask = pd.Series([True] * len(wo))
    if f_brigade != "All":
        mask &= wo["brigade"] == f_brigade
    if f_workshop != "All":
        mask &= wo["workshop"] == f_workshop
    if f_status != "All":
        mask &= wo["status"] == f_status
    if f_priority != "All":
        mask &= wo["priority"] == f_priority
    mask &= (wo["created_date"] >= f_date_from) & (wo["created_date"] <= f_date_to)
    filtered = wo[mask].reset_index(drop=True)

    st.markdown(f"**{len(filtered)} work orders found**")
    st.dataframe(filtered, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 🔍 Part Lines Drill-Down")
    for _, row in filtered.iterrows():
        lines = wpl[wpl["wo_id"] == row["wo_id"]]
        label = (f"**{row['wo_id']}** | {row['workshop']} | "
                 f"Priority: {row['priority']} | Status: {row['status']}")
        with st.expander(label):
            cols_show = ["line_id", "part_no", "part_desc",
                         "required_qty", "received_qty", "outstanding_qty", "line_status"]
            st.dataframe(lines[cols_show], use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# PAGE 2 — CREATE BATCH
# ─────────────────────────────────────────────────────────────
def page_create_batch():
    st.markdown('<div class="section-header">➕ Create Batch</div>', unsafe_allow_html=True)

    wo = st.session_state.work_orders
    wpl = st.session_state.wo_part_lines
    batches = st.session_state.batches
    batch_lines_df = st.session_state.batch_lines
    allocations = st.session_state.allocations

    # Determine which lines are already in an active batch
    active_statuses = {"Draft", "Subm to Procurement", "Under Procurement", "Partially Received"}
    active_batch_ids = batches[batches["batch_status"].isin(active_statuses)]["batch_id"].tolist()
    locked_line_ids = allocations[
        allocations["batch_line_id"].isin(
            batch_lines_df[batch_lines_df["batch_id"].isin(active_batch_ids)]["batch_line_id"].tolist()
        )
    ]["line_id"].tolist()

    # Brigade selector — uses real brigade list from CSV
    brigade = st.selectbox("Select Brigade", BRIGADES)

    eligible_wo_df = wo[
        (wo["brigade"] == brigade) & (wo["status"] == "Waiting Parts")
    ].copy()

    if eligible_wo_df.empty:
        st.warning("No eligible Work Orders (status: Waiting Parts) for this brigade.")
        _render_submit_drafts(brigade)
        return

    eligible_lines = wpl[
        (wpl["wo_id"].isin(eligible_wo_df["wo_id"])) &
        (~wpl["line_id"].isin(locked_line_ids)) &
        (wpl["line_status"] != "Ready")
    ].copy()
    eligible_lines = eligible_lines.merge(
        wo[["wo_id", "priority", "created_date", "workshop"]], on="wo_id", how="left"
    )

    st.markdown(f"#### Select Work Orders — **{brigade}**")
    st.caption("Tick the Work Orders you want to include in this batch. "
               "All eligible part lines for each selected WO will be added.")

    # ── Checkbox table ──────────────────────────────────────
    # Build a summary row per WO for the checkbox table
    wo_summary = (
        eligible_lines.groupby(["wo_id", "priority", "created_date", "workshop"], as_index=False)
        .agg(part_lines=("line_id", "count"), total_outstanding=("outstanding_qty", "sum"))
    ).sort_values(["priority", "created_date"], key=lambda col: col.map(PRIORITY_ORDER) if col.name == "priority" else col)

    # Initialise checkbox state
    if "cb_wo_sel" not in st.session_state:
        st.session_state.cb_wo_sel = {}

    # Header row
    hc0, hc1, hc2, hc3, hc4, hc5, hc6 = st.columns([0.5, 1.5, 1.5, 1.5, 1.5, 1.2, 1.2])
    hc0.markdown("**✓**")
    hc1.markdown("**WO ID**")
    hc2.markdown("**Workshop**")
    hc3.markdown("**Priority**")
    hc4.markdown("**Created**")
    hc5.markdown("**Lines**")
    hc6.markdown("**Outstanding**")

    st.markdown("<hr style='margin:4px 0 8px 0; border-color:#2d3150'>", unsafe_allow_html=True)

    priority_colors = {"Critical": "#f87171", "High": "#fbbf24", "Normal": "#94a3b8"}

    for _, row in wo_summary.iterrows():
        wid = row["wo_id"]
        key = f"cb_{wid}"
        # default to checked for first 3
        default = list(wo_summary["wo_id"]).index(wid) < 3
        c0, c1, c2, c3, c4, c5, c6 = st.columns([0.5, 1.5, 1.5, 1.5, 1.5, 1.2, 1.2])
        checked = c0.checkbox("", value=st.session_state.cb_wo_sel.get(key, default), key=key, label_visibility="collapsed")
        st.session_state.cb_wo_sel[key] = checked
        c1.markdown(f"`{wid}`")
        c2.markdown(row["workshop"])
        color = priority_colors.get(row["priority"], "#94a3b8")
        c3.markdown(f"<span style='color:{color};font-weight:600'>{row['priority']}</span>", unsafe_allow_html=True)
        c4.markdown(str(row["created_date"]))
        c5.markdown(str(int(row["part_lines"])))
        c6.markdown(f"**{int(row['total_outstanding'])}**")

    selected_wo = [
        row["wo_id"] for _, row in wo_summary.iterrows()
        if st.session_state.cb_wo_sel.get(f"cb_{row['wo_id']}", False)
    ]

    st.markdown("---")

    selected_lines = eligible_lines[eligible_lines["wo_id"].isin(selected_wo)].copy()

    if selected_lines.empty:
        st.info("Tick at least one Work Order above to continue.")
        _render_submit_drafts(brigade)
        return

    # Lines preview
    with st.expander(f"📋 View {len(selected_lines)} part lines included", expanded=False):
        st.dataframe(
            selected_lines[["line_id", "wo_id", "part_no", "part_desc", "outstanding_qty"]],
            use_container_width=True, hide_index=True,
        )

    # Aggregated batch lines
    agg = (
        selected_lines.groupby(["part_no", "part_desc"], as_index=False)["outstanding_qty"]
        .sum()
        .rename(columns={"outstanding_qty": "total_required_qty"})
    )
    st.markdown("**Aggregated Batch Lines (grouped by Part No):**")
    st.dataframe(agg, use_container_width=True, hide_index=True)

    col1, col2 = st.columns(2)
    with col1:
        approval_ref = st.text_input("Approval Reference", placeholder="e.g. APPREF-2024-001")
    with col2:
        created_by = st.text_input("Created By", placeholder="Your name")

    submit_to_proc = st.checkbox("Also submit to Procurement immediately")

    if st.button("✅ Create Batch", type="primary"):
        if not approval_ref.strip():
            st.error("Approval Reference is required.")
            return
        if not created_by.strip():
            st.error("Created By is required.")
            return

        new_batch_id = next_id("BATCH", st.session_state.batches["batch_id"].tolist())
        new_status = "Subm to Procurement" if submit_to_proc else "Draft"

        new_batch = pd.DataFrame([{
            "batch_id": new_batch_id,
            "brigade": brigade,
            "created_by": created_by.strip(),
            "created_date": date.today(),
            "approval_ref": approval_ref.strip(),
            "batch_status": new_status,
        }])
        st.session_state.batches = pd.concat(
            [st.session_state.batches, new_batch], ignore_index=True
        )

        new_batch_lines = []
        new_allocs = []
        for _, bl_row in agg.iterrows():
            bl_id = next_id(
                "BL",
                st.session_state.batch_lines["batch_line_id"].tolist()
                + [r["batch_line_id"] for r in new_batch_lines],
            )
            new_batch_lines.append({
                "batch_line_id": bl_id,
                "batch_id": new_batch_id,
                "part_no": bl_row["part_no"],
                "part_desc": bl_row["part_desc"],
                "total_required_qty": int(bl_row["total_required_qty"]),
                "vendor": "",
                "po_numbers": "",
                "ordered_qty": 0,
                "received_qty": 0,
                "expected_delivery_date": None,
            })
            part_lines = selected_lines[selected_lines["part_no"] == bl_row["part_no"]]
            for _, pl in part_lines.iterrows():
                alloc_id = next_id(
                    "ALLOC",
                    st.session_state.allocations["allocation_id"].tolist()
                    + [r["allocation_id"] for r in new_allocs],
                )
                new_allocs.append({
                    "allocation_id": alloc_id,
                    "batch_line_id": bl_id,
                    "wo_id": pl["wo_id"],
                    "line_id": pl["line_id"],
                    "allocated_qty": 0,
                    "allocation_status": "Allocated",
                    "last_updated": datetime.now(),
                    "notes": "",
                })

        if new_batch_lines:
            st.session_state.batch_lines = pd.concat(
                [st.session_state.batch_lines, pd.DataFrame(new_batch_lines)],
                ignore_index=True,
            )
        if new_allocs:
            st.session_state.allocations = pd.concat(
                [st.session_state.allocations, pd.DataFrame(new_allocs)],
                ignore_index=True,
            )

        # Clear checkbox state so next visit is fresh
        for key in list(st.session_state.cb_wo_sel.keys()):
            del st.session_state.cb_wo_sel[key]

        st.success(f"✅ Batch **{new_batch_id}** created with status **{new_status}**.")
        st.rerun()

    _render_submit_drafts(brigade)


def _render_submit_drafts(brigade: str):
    """Helper: show draft batches for this brigade with a Submit button."""
    st.markdown("---")
    st.markdown("#### 📤 Submit Draft Batches to Procurement")
    drafts = st.session_state.batches[
        (st.session_state.batches["batch_status"] == "Draft") &
        (st.session_state.batches["brigade"] == brigade)
    ]
    if drafts.empty:
        st.info("No draft batches for this brigade.")
    else:
        st.dataframe(drafts, use_container_width=True, hide_index=True)
        bid_to_submit = st.selectbox("Select Draft Batch", drafts["batch_id"].tolist(), key="submit_sel")
        if st.button("Submit to Procurement", key="submit_btn"):
            st.session_state.batches.loc[
                st.session_state.batches["batch_id"] == bid_to_submit, "batch_status"
            ] = "Subm to Procurement"
            st.success(f"Batch {bid_to_submit} submitted to procurement.")
            st.rerun()


# ─────────────────────────────────────────────────────────────
# PAGE 3 — PROCUREMENT UPDATES
# ─────────────────────────────────────────────────────────────
def page_procurement_updates():
    st.markdown('<div class="section-header">📦 Procurement Updates</div>', unsafe_allow_html=True)

    active_statuses = ["Subm to Procurement", "Under Procurement",
                       "Partially Received", "Fully Received"]
    eligible_batches = st.session_state.batches[
        st.session_state.batches["batch_status"].isin(active_statuses)
    ]

    if eligible_batches.empty:
        st.warning("No batches available for procurement updates. Submit a batch first.")
        return

    batch_options = eligible_batches["batch_id"].tolist()
    selected_batch = st.selectbox("Select Batch", batch_options)

    batch_info = st.session_state.batches[
        st.session_state.batches["batch_id"] == selected_batch
    ].iloc[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Brigade", batch_info["brigade"])
    col2.metric("Status", batch_info["batch_status"])
    col3.metric("Approval Ref", batch_info["approval_ref"])

    st.markdown("#### Batch Lines — Edit Procurement Details")

    bl = st.session_state.batch_lines[
        st.session_state.batch_lines["batch_id"] == selected_batch
    ].copy()

    if bl.empty:
        st.info("No batch lines found.")
        return

    # Make editable
    edited = st.data_editor(
        bl[["batch_line_id", "part_no", "part_desc", "total_required_qty",
            "vendor", "po_numbers", "ordered_qty", "received_qty", "expected_delivery_date"]],
        use_container_width=True,
        hide_index=True,
        disabled=["batch_line_id", "part_no", "part_desc", "total_required_qty"],
        column_config={
            "received_qty": st.column_config.NumberColumn("Received Qty", min_value=0),
            "ordered_qty": st.column_config.NumberColumn("Ordered Qty", min_value=0),
            "expected_delivery_date": st.column_config.DateColumn("Expected Delivery"),
        },
        key=f"proc_editor_{selected_batch}",
    )

    if st.button("💾 Save Procurement Updates", type="primary"):
        for _, erow in edited.iterrows():
            bl_id = erow["batch_line_id"]
            mask = st.session_state.batch_lines["batch_line_id"] == bl_id

            old_rec = int(
                st.session_state.batch_lines.loc[mask, "received_qty"].values[0]
            )
            new_rec = int(erow["received_qty"])

            st.session_state.batch_lines.loc[mask, "vendor"] = erow["vendor"]
            st.session_state.batch_lines.loc[mask, "po_numbers"] = erow["po_numbers"]
            st.session_state.batch_lines.loc[mask, "ordered_qty"] = int(erow["ordered_qty"])
            st.session_state.batch_lines.loc[mask, "received_qty"] = new_rec
            st.session_state.batch_lines.loc[mask, "expected_delivery_date"] = erow["expected_delivery_date"]

            # Trigger allocation engine if received_qty changed
            if new_rec != old_rec:
                run_allocation_engine(bl_id)

        # Recalculate batch status
        new_bs = derive_batch_status_from_lines(selected_batch)
        # Only update if currently under procurement or partially received
        current_status = batch_info["batch_status"]
        if current_status not in ("Closed",):
            st.session_state.batches.loc[
                st.session_state.batches["batch_id"] == selected_batch, "batch_status"
            ] = new_bs

        st.success("✅ Procurement details saved. Allocation engine has been re-run.")
        st.rerun()

    # Show current allocations summary
    st.markdown("---")
    st.markdown("#### 📊 Current Allocation Summary")
    allocs = st.session_state.allocations[
        st.session_state.allocations["batch_line_id"].isin(bl["batch_line_id"].tolist())
    ].copy()

    if not allocs.empty:
        allocs = allocs.merge(
            st.session_state.work_orders[["wo_id", "priority"]], on="wo_id", how="left"
        )
        allocs = allocs.merge(
            st.session_state.wo_part_lines[["line_id", "part_no", "required_qty", "received_qty",
                                            "outstanding_qty", "line_status"]],
            on="line_id", how="left"
        )
        st.dataframe(
            allocs[["batch_line_id", "wo_id", "priority", "part_no",
                    "required_qty", "allocated_qty", "outstanding_qty", "allocation_status"]],
            use_container_width=True, hide_index=True,
        )


# ─────────────────────────────────────────────────────────────
# PAGE 4 — ALLOCATION & PACKING
# ─────────────────────────────────────────────────────────────
def page_allocation_packing():
    st.markdown('<div class="section-header">🗂️ Allocation & Packing</div>', unsafe_allow_html=True)

    batches = st.session_state.batches
    if batches.empty:
        st.warning("No batches exist yet.")
        return

    batch_sel = st.selectbox("Select Batch", batches["batch_id"].tolist())

    bl = st.session_state.batch_lines[
        st.session_state.batch_lines["batch_id"] == batch_sel
    ]
    if bl.empty:
        st.info("No batch lines for this batch.")
        return

    bl_sel = st.selectbox("Select Batch Line", bl["batch_line_id"].tolist())

    bl_info = bl[bl["batch_line_id"] == bl_sel].iloc[0]
    total_received = int(bl_info["received_qty"])

    st.markdown(f"**Part:** {bl_info['part_no']} — {bl_info['part_desc']} | "
                f"**Total Required:** {bl_info['total_required_qty']} | "
                f"**Received:** {total_received}")

    # Allocation grid
    allocs = st.session_state.allocations[
        st.session_state.allocations["batch_line_id"] == bl_sel
    ].copy()

    if allocs.empty:
        st.info("No allocations for this batch line.")
        return

    allocs = allocs.merge(
        st.session_state.work_orders[["wo_id", "priority", "created_date", "workshop"]],
        on="wo_id", how="left",
    )
    allocs = allocs.merge(
        st.session_state.wo_part_lines[["line_id", "required_qty", "received_qty",
                                        "outstanding_qty"]],
        on="line_id", how="left",
    )
    allocs["outstanding_qty_wo"] = (
        allocs["required_qty"] - (allocs["received_qty"] - allocs["allocated_qty"])
    ).clip(lower=0)

    cols_edit = ["allocation_id", "wo_id", "priority", "created_date",
                 "required_qty", "received_qty", "outstanding_qty", "allocated_qty",
                 "allocation_status", "notes"]

    st.markdown("#### ✏️ Manual Allocation Override")
    st.caption(f"Total received for this batch line: **{total_received}** units. "
               f"Sum of allocated_qty cannot exceed this.")

    edited_allocs = st.data_editor(
        allocs[cols_edit],
        use_container_width=True,
        hide_index=True,
        disabled=["allocation_id", "wo_id", "priority", "created_date",
                  "required_qty", "received_qty", "outstanding_qty"],
        column_config={
            "allocated_qty": st.column_config.NumberColumn("Allocated Qty", min_value=0),
            "allocation_status": st.column_config.SelectboxColumn(
                "Status",
                options=["Allocated", "Packed", "Collected", "DeliveredConfirmed"],
            ),
        },
        key=f"alloc_editor_{bl_sel}",
    )

    if st.button("💾 Save Allocation Overrides", type="primary"):
        new_total = int(edited_allocs["allocated_qty"].sum())
        if new_total > total_received:
            st.error(f"❌ Total allocated ({new_total}) exceeds received qty ({total_received}).")
            return

        for _, erow in edited_allocs.iterrows():
            alloc_id = erow["allocation_id"]
            line_id = allocs.loc[allocs["allocation_id"] == alloc_id, "line_id"].values[0]
            required = int(allocs.loc[allocs["allocation_id"] == alloc_id, "required_qty"].values[0])

            give = int(erow["allocated_qty"])
            if give > required:
                st.error(f"Allocated qty for {erow['wo_id']} exceeds required qty ({required}).")
                return

            mask = st.session_state.allocations["allocation_id"] == alloc_id
            st.session_state.allocations.loc[mask, "allocated_qty"] = give
            st.session_state.allocations.loc[mask, "allocation_status"] = erow["allocation_status"]
            st.session_state.allocations.loc[mask, "notes"] = erow["notes"]
            st.session_state.allocations.loc[mask, "last_updated"] = datetime.now()

            # Update WO part line received_qty = sum of all allocations for that line
            line_allocs_total = st.session_state.allocations[
                st.session_state.allocations["line_id"] == line_id
            ]["allocated_qty"].sum()
            st.session_state.wo_part_lines.loc[
                st.session_state.wo_part_lines["line_id"] == line_id, "received_qty"
            ] = int(line_allocs_total)

        st.session_state.wo_part_lines = derive_wo_part_lines(st.session_state.wo_part_lines)
        st.success("✅ Allocation overrides saved.")
        st.rerun()

    # ── Packing Lists ─────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 📦 Packing Lists")

    tab1, tab2 = st.tabs(["Per-WO Packing List", "Collection Manifest (Full Batch)"])

    with tab1:
        wo_options = allocs["wo_id"].unique().tolist()
        sel_wo = st.selectbox("Select Work Order", wo_options, key="pack_wo")

        batch_allocs_all = st.session_state.allocations[
            st.session_state.allocations["batch_line_id"].isin(bl["batch_line_id"].tolist())
        ].copy()
        batch_allocs_wo = batch_allocs_all[batch_allocs_all["wo_id"] == sel_wo]
        packing = batch_allocs_wo.merge(
            st.session_state.batch_lines[["batch_line_id", "part_no", "part_desc"]],
            on="batch_line_id", how="left",
        )
        st.markdown(f"**Packing List for {sel_wo} — Batch {batch_sel}**")
        st.dataframe(
            packing[["part_no", "part_desc", "allocated_qty", "allocation_status"]],
            use_container_width=True, hide_index=True,
        )

    with tab2:
        st.markdown(f"**Collection Manifest — Batch {batch_sel}**")
        all_allocs = st.session_state.allocations[
            st.session_state.allocations["batch_line_id"].isin(bl["batch_line_id"].tolist())
        ].copy()
        manifest = all_allocs.merge(
            st.session_state.batch_lines[["batch_line_id", "part_no", "part_desc"]],
            on="batch_line_id", how="left",
        )
        manifest = manifest[["wo_id", "part_no", "part_desc", "allocated_qty", "allocation_status"]]
        manifest = manifest.sort_values(["wo_id", "part_no"])
        st.dataframe(manifest, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# PAGE 5 — DASHBOARDS
# ─────────────────────────────────────────────────────────────
def kpi(label: str, value, delta=None):
    d_str = f'<div class="kpi-label">{delta}</div>' if delta else ""
    st.markdown(
        f'<div class="kpi-card"><div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>{d_str}</div>',
        unsafe_allow_html=True,
    )


def page_dashboards():
    st.markdown('<div class="section-header">📊 Dashboards</div>', unsafe_allow_html=True)

    wo = st.session_state.work_orders
    wpl = derive_wo_part_lines(st.session_state.wo_part_lines)
    batches = st.session_state.batches

    # Top KPIs
    open_wo = wo[wo["status"] == "Waiting Parts"]
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi("Open Work Orders", len(open_wo))
    with c2:
        kpi("Total Batches", len(batches))
    with c3:
        ready = wpl[wpl["line_status"] == "Ready"]
        kpi("Part Lines Ready", len(ready))
    with c4:
        waiting = wpl[wpl["line_status"] == "Waiting"]
        kpi("Part Lines Waiting", len(waiting))

    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### Open WOs by Workshop")
        ow_by_ws = (
            open_wo.groupby("workshop").size().reset_index(name="Count")
        )
        st.dataframe(ow_by_ws, use_container_width=True, hide_index=True)

        st.markdown("#### Avg WO Age by Workshop (days)")
        wo_copy = wo.copy()
        wo_copy["age_days"] = (date.today() - wo_copy["created_date"]).apply(
            lambda x: x.days if hasattr(x, "days") else 0
        )
        avg_age = wo_copy.groupby("workshop")["age_days"].mean().reset_index()
        avg_age.columns = ["Workshop", "Avg Age (days)"]
        avg_age["Avg Age (days)"] = avg_age["Avg Age (days)"].round(1)
        st.dataframe(avg_age, use_container_width=True, hide_index=True)

    with col_b:
        st.markdown("#### Part Line Status Summary")
        ls_counts = wpl["line_status"].value_counts().reset_index()
        ls_counts.columns = ["Status", "Count"]
        st.dataframe(ls_counts, use_container_width=True, hide_index=True)

        st.markdown("#### Batches by Status")
        if batches.empty:
            st.info("No batches created yet.")
        else:
            bs_counts = batches["batch_status"].value_counts().reset_index()
            bs_counts.columns = ["Status", "Count"]
            st.dataframe(bs_counts, use_container_width=True, hide_index=True)

    # Days in current batch status
    if not batches.empty:
        st.markdown("---")
        st.markdown("#### ⏱️ Days in Current Batch Status")
        bst = batches.copy()
        bst["days_in_status"] = (
            pd.Timestamp.today() - pd.to_datetime(bst["created_date"])
        ).dt.days
        st.dataframe(
            bst[["batch_id", "brigade", "batch_status", "days_in_status"]],
            use_container_width=True, hide_index=True,
        )

    # Priority distribution
    st.markdown("---")
    st.markdown("#### Priority Distribution of Open WOs")
    prio_dist = (
        open_wo.groupby("priority").size()
        .reindex(["Critical", "High", "Normal"], fill_value=0)
        .reset_index()
    )
    prio_dist.columns = ["Priority", "Count"]
    st.dataframe(prio_dist, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    maybe_init()
    page = sidebar()

    if page == "📋 Work Orders":
        page_work_orders()
    elif page == "➕ Create Batch":
        page_create_batch()
    elif page == "📦 Procurement Updates":
        page_procurement_updates()
    elif page == "🗂️ Allocation & Packing":
        page_allocation_packing()
    elif page == "📊 Dashboards":
        page_dashboards()


if __name__ == "__main__":
    main()
