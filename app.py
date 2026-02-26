"""
IMSS Batch Management & Allocation Module
==========================================
Three logical layers (single file):

  ┌── DATA LAYER ──────────────────────────────────────────────┐
  │  dl_*  functions — all session_state reads/writes          │
  │  TO SWAP DB: change only these functions                   │
  ├── SERVICE LAYER ───────────────────────────────────────────┤
  │  svc_* functions — business logic, validation, allocation  │
  │  Calls data_layer; never touches session_state directly    │
  ├── UI LAYER ────────────────────────────────────────────────┤
  │  page_* functions — Streamlit pages                        │
  │  Calls service_layer only; never mutates DataFrames        │
  └────────────────────────────────────────────────────────────┘
"""

# ════════════════════════════════════════════════════════════════
# IMPORTS & PAGE CONFIG
# ════════════════════════════════════════════════════════════════
import os
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="IMSS Batch Management",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
[data-testid="stSidebar"]{background:#0f1117!important;border-right:1px solid #2a2d3a;}
[data-testid="stSidebar"] *{color:#e0e4f0!important;}
[data-testid="stSidebar"] .stRadio label{padding:6px 10px;border-radius:4px;cursor:pointer;}
[data-testid="stSidebar"] .stRadio label:hover{background:#1e2130;}
.kpi-card{background:#1a1d2e;border:1px solid #2d3150;border-left:4px solid #4f8ef7;
  border-radius:6px;padding:16px 20px;margin-bottom:8px;}
.kpi-value{font-size:2rem;font-weight:700;color:#4f8ef7;font-family:'IBM Plex Mono';}
.kpi-label{font-size:.78rem;color:#7880a0;text-transform:uppercase;letter-spacing:.08em;}
.kpi-card.red  {border-left-color:#f87171;} .kpi-card.red  .kpi-value{color:#f87171;}
.kpi-card.amber{border-left-color:#fbbf24;} .kpi-card.amber.kpi-value{color:#fbbf24;}
.kpi-card.green{border-left-color:#4ade80;} .kpi-card.green .kpi-value{color:#4ade80;}
.section-header{font-size:1.3rem;font-weight:700;color:#e0e4f0;
  border-bottom:2px solid #2d3150;padding-bottom:8px;margin-bottom:16px;
  font-family:'IBM Plex Mono';}
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
# CONSTANTS & STATIC CONFIG
# ════════════════════════════════════════════════════════════════
PRIORITY_ORDER = {"Critical": 0, "High": 1, "Normal": 2}

BATCH_TRANSITIONS: dict = {
    "Draft":               ["Subm to Procurement"],
    "Subm to Procurement": ["Under Procurement"],
    "Under Procurement":   ["Partially Received"],
    "Partially Received":  ["Fully Received"],
    "Fully Received":      ["Closed"],
    "Closed":              [],
}

RESPONSIBILITY_OWNERS = [
    "AIC Procurement", "Military Approval", "Military Transport", "Workshop",
]
ALLOCATION_MODES = [
    "Priority First then FIFO", "FIFO", "Manual Only",
]
EXCEPTION_TYPES = [
    "Obsolete", "Cancelled", "Rebatch", "Vendor Rejected", "Military Delay",
]


# ════════════════════════════════════════════════════════════════
# STATIC CSV LOADERS  (replace with DB queries in data_layer)
# ════════════════════════════════════════════════════════════════
def _csv_path(name: str) -> str:
    return os.path.join(os.path.dirname(__file__), name)


def load_brigades() -> list:
    p = _csv_path("Brigades_20260226.csv")
    if not os.path.exists(p):
        return [
            "KAMB — King Abdulaziz Mechanized Brigade",
            "IMSMB — Imam Muhammad bin Saud Mechanized Brigade",
        ]
    df = pd.read_csv(p, comment="#")
    df.columns = [c.strip() for c in df.columns]
    return sorted((df["Code"].str.strip() + " — " + df["NameEn"].str.strip()).tolist())


@st.cache_data(show_spinner="Loading parts catalogue…")
def load_parts() -> pd.DataFrame:
    p = _csv_path("materials_export_2026-02-26.csv")
    if not os.path.exists(p):
        return pd.DataFrame(columns=[
            "MNGPartNumber", "DescriptionEn", "DescriptionAr", "UnitOfMeasure",
            "Criticality", "PlatformVehicleType", "Supplier", "UnitPrice",
            "NSN", "OEMPartNumber", "WarehouseCategory", "Repairability",
            "LeadTimeDays", "MinStockLevel", "MaxStockLevel",
        ])
    keep = [
        "MNGPartNumber", "DescriptionEn", "DescriptionAr", "UnitOfMeasure",
        "Criticality", "PlatformVehicleType", "Supplier", "UnitPrice",
        "NSN", "OEMPartNumber", "WarehouseCategory", "Repairability",
        "LeadTimeDays", "MinStockLevel", "MaxStockLevel",
    ]
    df = pd.read_csv(p, comment="#", encoding="utf-8-sig", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    df = df[[c for c in keep if c in df.columns]].copy()
    df["MNGPartNumber"] = df["MNGPartNumber"].astype(str).str.strip()
    df["DescriptionEn"] = df["DescriptionEn"].astype(str).str.strip()
    mask = (
        (df["DescriptionEn"].str.len() > 3)
        & (~df["DescriptionEn"].isin(["0", "N/A", "NA", "nan"]))
        & (df["MNGPartNumber"].str.len() > 3)
    )
    df = df[mask].drop_duplicates("MNGPartNumber").reset_index(drop=True)
    df["label"] = df["MNGPartNumber"] + " — " + df["DescriptionEn"]
    return df


BRIGADES: list = load_brigades()
PARTS_DF: pd.DataFrame = load_parts()


# ════════════════════════════════════════════════════════════════
# ══ DATA LAYER ══════════════════════════════════════════════════
# All session_state I/O.  Replace each function with a DB call
# (SQLAlchemy/psycopg2) returning the same structure.
# ════════════════════════════════════════════════════════════════

def dl_get(table: str) -> pd.DataFrame:
    """Read table. DB: SELECT * FROM <table>"""
    return st.session_state[table]


def dl_set(table: str, df: pd.DataFrame) -> None:
    """Replace whole table. DB: handled by granular update/insert."""
    st.session_state[table] = df


def dl_update_rows(table: str, mask: pd.Series, updates: dict) -> None:
    """Update rows matching mask. DB: UPDATE <table> SET ... WHERE ..."""
    df = dl_get(table).copy()
    for col, val in updates.items():
        df.loc[mask, col] = val
    dl_set(table, df)


def dl_append(table: str, rows: list) -> None:
    """Insert rows. DB: INSERT INTO <table> ..."""
    dl_set(table, pd.concat([dl_get(table), pd.DataFrame(rows)], ignore_index=True))


def dl_get_config() -> dict:
    """Read system config. DB: SELECT * FROM system_config"""
    return st.session_state.get("config", {})


def dl_set_config(key: str, val) -> None:
    """Write config value. DB: UPSERT system_config SET value=... WHERE key=..."""
    cfg = dl_get_config().copy()
    cfg[key] = val
    st.session_state["config"] = cfg


def dl_next_id(prefix: str, table: str, id_col: str) -> str:
    """Generate next sequential ID. DB: sequence / SERIAL column."""
    existing = dl_get(table)[id_col].tolist() if not dl_get(table).empty else []
    return _next_id_from_list(prefix, existing)


def _next_id_from_list(prefix: str, existing: list) -> str:
    nums = []
    for eid in existing:
        try:
            nums.append(int(str(eid).replace(f"{prefix}-", "")))
        except (ValueError, AttributeError):
            pass
    return f"{prefix}-{(max(nums) + 1 if nums else 1):04d}"


def dl_audit(
    entity_type: str,
    entity_id: str,
    action: str,
    old_value: str = "",
    new_value: str = "",
    changed_by: str = "System",
) -> None:
    """Append audit entry. DB: INSERT INTO audit_log ..."""
    row = {
        "audit_id":    dl_next_id("AUD", "audit_log", "audit_id"),
        "entity_type": entity_type,
        "entity_id":   entity_id,
        "action":      action,
        "old_value":   str(old_value)[:500],
        "new_value":   str(new_value)[:500],
        "changed_by":  changed_by,
        "timestamp":   datetime.now(),
    }
    dl_append("audit_log", [row])


# ════════════════════════════════════════════════════════════════
# ══ SERVICE LAYER ═══════════════════════════════════════════════
# Pure business logic. Never touches session_state directly.
# All persistence goes through the data_layer (dl_* functions).
# ════════════════════════════════════════════════════════════════

class ValidationError(Exception):
    """Raised when a business rule is violated."""


# ── Derived fields ────────────────────────────────────────────
def svc_derive_wo_part_lines(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["outstanding_qty"] = (df["required_qty"] - df["received_qty"]).clip(lower=0)
    conditions = [
        df["received_qty"] >= df["required_qty"],
        df["received_qty"] > 0,
    ]
    choices = ["Ready", "Partial"]
    df["line_status"] = pd.np.select(conditions, choices, default="Waiting") \
        if hasattr(pd, "np") \
        else df.apply(
            lambda r: "Ready" if r["received_qty"] >= r["required_qty"]
            else ("Partial" if r["received_qty"] > 0 else "Waiting"),
            axis=1,
        )
    return df


# ── Status state machine ──────────────────────────────────────
def svc_can_transition(current: str, new: str) -> bool:
    return new in BATCH_TRANSITIONS.get(current, [])


def svc_transition_batch(
    batch_id: str, new_status: str, changed_by: str = "System"
) -> tuple:
    batches = dl_get("batches")
    row = batches[batches["batch_id"] == batch_id]
    if row.empty:
        return False, f"Batch {batch_id} not found."
    current = row.iloc[0]["batch_status"]
    if not svc_can_transition(current, new_status):
        return False, (
            f"Cannot transition '{current}' → '{new_status}'. "
            f"Allowed: {BATCH_TRANSITIONS.get(current, [])}"
        )
    dl_audit("Batch", batch_id, "STATUS_CHANGE", current, new_status, changed_by)
    dl_update_rows("batches", batches["batch_id"] == batch_id, {"batch_status": new_status})
    return True, f"Batch {batch_id} → '{new_status}'."


# ── Integrity validators ──────────────────────────────────────
def svc_validate_no_duplicate_active_lines(line_ids: list) -> None:
    """A WO part line cannot belong to more than one active batch."""
    active_statuses = {
        "Draft", "Subm to Procurement", "Under Procurement", "Partially Received",
    }
    active_bids = dl_get("batches")[
        dl_get("batches")["batch_status"].isin(active_statuses)
    ]["batch_id"].tolist()
    bl = dl_get("batch_lines")
    active_bl_ids = bl[bl["batch_id"].isin(active_bids)]["batch_line_id"].tolist()
    locked = dl_get("allocations")[
        dl_get("allocations")["batch_line_id"].isin(active_bl_ids)
    ]["line_id"].tolist()
    conflicts = set(line_ids) & set(locked)
    if conflicts:
        raise ValidationError(
            f"Lines {sorted(conflicts)} are already in an active batch."
        )


def svc_validate_single_brigade(wo_ids: list, brigade: str) -> None:
    """A batch cannot mix brigades."""
    brigades = dl_get("work_orders").set_index("wo_id")["brigade"]
    wrong = [w for w in wo_ids if brigades.get(w) != brigade]
    if wrong:
        raise ValidationError(f"WOs {wrong} do not belong to brigade '{brigade}'.")


def svc_validate_batch_not_closed(batch_id: str) -> None:
    b = dl_get("batches")[dl_get("batches")["batch_id"] == batch_id]
    if not b.empty and b.iloc[0]["batch_status"] == "Closed":
        raise ValidationError(f"Batch {batch_id} is Closed and cannot be edited.")


def svc_validate_received_not_below_allocated(batch_line_id: str, new_received: int) -> None:
    total_alloc = int(
        dl_get("allocations")[
            dl_get("allocations")["batch_line_id"] == batch_line_id
        ]["allocated_qty"].sum()
    )
    if new_received < total_alloc:
        raise ValidationError(
            f"Cannot set received_qty={new_received}: "
            f"total already allocated={total_alloc}. Reduce allocations first."
        )


def svc_validate_fully_received(batch_id: str) -> None:
    lines = dl_get("batch_lines")[dl_get("batch_lines")["batch_id"] == batch_id]
    outstanding = int((lines["total_required_qty"] - lines["received_qty"]).clip(lower=0).sum())
    if outstanding > 0:
        raise ValidationError(
            f"Cannot mark Fully Received: {outstanding} units still outstanding."
        )


# ── Allocation engine (vectorised, delta-based) ───────────────
def svc_run_allocation_engine(
    batch_line_id: str, new_received: int, old_received: int
) -> None:
    """
    Delta-based, vectorised allocation engine.

    Rules:
    - delta = new_received - old_received; only distributes delta.
    - Positive delta: fill outstanding from highest-priority / oldest WO first.
    - Negative delta: reduce from lowest-priority last; never touch ManualOverride.
    - Respects AllocationMode setting (Priority+FIFO | FIFO | Manual Only).
    - Performance: O(n) vectorised merge; row loop only for sequential fill.
    """
    mode = dl_get_config().get("allocation_mode", "Priority First then FIFO")
    if mode == "Manual Only":
        return

    delta = new_received - old_received
    if delta == 0:
        return

    # ── Vectorised data assembly ──────────────────────────────
    allocs = dl_get("allocations")
    mask_bl = allocs["batch_line_id"] == batch_line_id
    batch_allocs = allocs[mask_bl].copy()
    if batch_allocs.empty:
        return

    wo_info = dl_get("work_orders")[["wo_id", "priority", "created_date"]]
    wpl_info = dl_get("wo_part_lines")[["line_id", "required_qty"]]

    batch_allocs = (
        batch_allocs
        .merge(wo_info, on="wo_id", how="left")
        .merge(wpl_info, on="line_id", how="left")
    )
    batch_allocs["outstanding"] = (
        batch_allocs["required_qty"] - batch_allocs["allocated_qty"]
    ).clip(lower=0)

    # ── Sort ──────────────────────────────────────────────────
    if mode == "Priority First then FIFO":
        batch_allocs["_prank"] = batch_allocs["priority"].map(PRIORITY_ORDER).fillna(9)
        batch_allocs = batch_allocs.sort_values(["_prank", "created_date"]).reset_index(drop=True)
    else:
        batch_allocs = batch_allocs.sort_values("created_date").reset_index(drop=True)

    # ── Distribute delta (minimal loop — inherently sequential) ─
    if delta > 0:
        remaining = int(delta)
        for idx in range(len(batch_allocs)):
            if remaining <= 0:
                break
            give = min(int(batch_allocs.at[idx, "outstanding"]), remaining)
            batch_allocs.at[idx, "allocated_qty"] = int(batch_allocs.at[idx, "allocated_qty"]) + give
            remaining -= give
    else:
        to_reduce = abs(int(delta))
        for idx in reversed(range(len(batch_allocs))):
            if to_reduce <= 0:
                break
            if batch_allocs.at[idx, "allocation_status"] == "ManualOverride":
                continue  # never auto-reduce manual overrides
            cur = int(batch_allocs.at[idx, "allocated_qty"])
            reduce_by = min(cur, to_reduce)
            batch_allocs.at[idx, "allocated_qty"] = cur - reduce_by
            to_reduce -= reduce_by

    # ── Vectorised write-back via index alignment ─────────────
    updated_qty = batch_allocs.set_index("allocation_id")["allocated_qty"]
    allocs_copy = dl_get("allocations").copy().set_index("allocation_id")
    allocs_copy.loc[updated_qty.index, "allocated_qty"] = updated_qty.values
    allocs_copy.loc[updated_qty.index, "last_updated"] = datetime.now()
    dl_set("allocations", allocs_copy.reset_index())

    # ── Update WO part lines received_qty (vectorised groupby) ─
    affected_lines = batch_allocs["line_id"].unique()
    final_allocs = dl_get("allocations")
    line_totals = (
        final_allocs[final_allocs["line_id"].isin(affected_lines)]
        .groupby("line_id")["allocated_qty"]
        .sum()
    )
    wpl = dl_get("wo_part_lines").copy().set_index("line_id")
    wpl.loc[wpl.index.isin(line_totals.index), "received_qty"] = line_totals
    dl_set("wo_part_lines", svc_derive_wo_part_lines(wpl.reset_index()))


def svc_reset_allocation_to_auto(batch_line_id: str, changed_by: str = "System") -> None:
    """Zero all allocations for batch line, then re-run engine from scratch."""
    allocs = dl_get("allocations").copy()
    mask = allocs["batch_line_id"] == batch_line_id
    affected_lines = allocs.loc[mask, "line_id"].tolist()
    allocs.loc[mask, "allocated_qty"] = 0
    allocs.loc[mask, "allocation_status"] = "Allocated"
    dl_set("allocations", allocs)

    wpl = dl_get("wo_part_lines").copy()
    for lid in affected_lines:
        wpl.loc[wpl["line_id"] == lid, "received_qty"] = 0
    dl_set("wo_part_lines", svc_derive_wo_part_lines(wpl))

    received = int(
        dl_get("batch_lines")
        .loc[dl_get("batch_lines")["batch_line_id"] == batch_line_id, "received_qty"]
        .values[0]
    )
    svc_run_allocation_engine(batch_line_id, received, 0)
    dl_audit("BatchLine", batch_line_id, "RESET_TO_AUTO", "", f"received={received}", changed_by)


def svc_recalc_batch_status(batch_id: str) -> str:
    lines = dl_get("batch_lines")[dl_get("batch_lines")["batch_id"] == batch_id]
    if lines.empty:
        return "Draft"
    total_req = int(lines["total_required_qty"].sum())
    total_rec = int(lines["received_qty"].sum())
    if total_rec == 0:
        return "Under Procurement"
    if total_rec >= total_req:
        return "Fully Received"
    return "Partially Received"


# ── Create batch ──────────────────────────────────────────────
def svc_create_batch(
    brigade: str,
    selected_wo_ids: list,
    approval_ref: str,
    created_by: str,
    submit_immediately: bool,
) -> tuple:
    """Validate and create batch + batch lines + allocation stubs."""
    wpl = dl_get("wo_part_lines")
    eligible = wpl[wpl["wo_id"].isin(selected_wo_ids)].copy()

    try:
        svc_validate_single_brigade(selected_wo_ids, brigade)
        svc_validate_no_duplicate_active_lines(eligible["line_id"].tolist())
    except ValidationError as exc:
        return False, str(exc)

    new_batch_id = dl_next_id("BATCH", "batches", "batch_id")
    status = "Subm to Procurement" if submit_immediately else "Draft"

    dl_append("batches", [{
        "batch_id":             new_batch_id,
        "brigade":              brigade,
        "created_by":           created_by,
        "created_date":         date.today(),
        "approval_ref":         approval_ref,
        "batch_status":         status,
        "responsibility_owner": "AIC Procurement",
        "owner_since":          datetime.now(),
    }])

    agg = (
        eligible
        .groupby(["part_no", "part_desc"], as_index=False)["outstanding_qty"]
        .sum()
        .rename(columns={"outstanding_qty": "total_required_qty"})
    )

    existing_bl: list = []
    existing_al: list = []
    new_blines: list = []
    new_allocs: list = []

    for _, bl_row in agg.iterrows():
        bl_id = _next_id_from_list("BL", dl_get("batch_lines")["batch_line_id"].tolist() + existing_bl)
        existing_bl.append(bl_id)
        new_blines.append({
            "batch_line_id":        bl_id,
            "batch_id":             new_batch_id,
            "part_no":              bl_row["part_no"],
            "part_desc":            bl_row["part_desc"],
            "total_required_qty":   int(bl_row["total_required_qty"]),
            "vendor":               "",
            "po_numbers":           "",
            "ordered_qty":          0,
            "received_qty":         0,
            "expected_delivery_date": None,
        })
        for _, pl in eligible[eligible["part_no"] == bl_row["part_no"]].iterrows():
            a_id = _next_id_from_list("ALLOC", dl_get("allocations")["allocation_id"].tolist() + existing_al)
            existing_al.append(a_id)
            new_allocs.append({
                "allocation_id":        a_id,
                "batch_line_id":        bl_id,
                "wo_id":                pl["wo_id"],
                "line_id":              pl["line_id"],
                "allocated_qty":        0,
                "allocation_status":    "Allocated",
                "last_updated":         datetime.now(),
                "notes":                "",
                "responsibility_owner": "AIC Procurement",
                "owner_since":          datetime.now(),
            })

    if new_blines:
        dl_append("batch_lines", new_blines)
    if new_allocs:
        dl_append("allocations", new_allocs)

    dl_audit(
        "Batch", new_batch_id, "CREATED", "",
        f"brigade={brigade} WOs={selected_wo_ids} status={status}",
        created_by,
    )
    return True, new_batch_id


# ── Update procurement line ───────────────────────────────────
def svc_update_procurement_line(
    batch_line_id: str,
    vendor: str,
    po_numbers: str,
    ordered_qty: int,
    new_received: int,
    expected_delivery_date,
    changed_by: str = "System",
) -> tuple:
    bl = dl_get("batch_lines")
    row = bl[bl["batch_line_id"] == batch_line_id]
    if row.empty:
        return False, "Batch line not found."

    batch_id = row.iloc[0]["batch_id"]
    old_received = int(row.iloc[0]["received_qty"])

    try:
        svc_validate_batch_not_closed(batch_id)
        svc_validate_received_not_below_allocated(batch_line_id, new_received)
    except ValidationError as exc:
        return False, str(exc)

    dl_audit(
        "BatchLine", batch_line_id, "PROCUREMENT_UPDATE",
        f"received={old_received}", f"received={new_received}", changed_by,
    )
    dl_update_rows("batch_lines", bl["batch_line_id"] == batch_line_id, {
        "vendor":                   vendor,
        "po_numbers":               po_numbers,
        "ordered_qty":              ordered_qty,
        "received_qty":             new_received,
        "expected_delivery_date":   expected_delivery_date,
    })

    if new_received != old_received:
        svc_run_allocation_engine(batch_line_id, new_received, old_received)

    # Auto-advance batch status if appropriate
    new_bs = svc_recalc_batch_status(batch_id)
    current_bs = dl_get("batches").loc[
        dl_get("batches")["batch_id"] == batch_id, "batch_status"
    ].values[0]
    if current_bs not in ("Closed", "Draft", "Subm to Procurement") and new_bs != current_bs:
        dl_update_rows("batches", dl_get("batches")["batch_id"] == batch_id,
                       {"batch_status": new_bs})
        dl_audit("Batch", batch_id, "STATUS_AUTO", current_bs, new_bs, "System")

    return True, "Saved."


# ── Apply allocation override ─────────────────────────────────
def svc_apply_allocation_override(
    overrides: pd.DataFrame, batch_line_id: str, changed_by: str = "System"
) -> tuple:
    bl = dl_get("batch_lines")[dl_get("batch_lines")["batch_line_id"] == batch_line_id]
    if bl.empty:
        return False, "Batch line not found."
    total_received = int(bl.iloc[0]["received_qty"])
    new_total = int(overrides["allocated_qty"].sum())
    if new_total > total_received:
        return False, f"Total allocated ({new_total}) exceeds received qty ({total_received})."

    allocs = dl_get("allocations")
    wpl = dl_get("wo_part_lines").copy()

    for _, erow in overrides.iterrows():
        a_id = erow["allocation_id"]
        orig = allocs[allocs["allocation_id"] == a_id]
        if orig.empty:
            continue
        line_id = orig.iloc[0]["line_id"]
        req = int(
            wpl.loc[wpl["line_id"] == line_id, "required_qty"].values[0]
        )
        give = int(erow["allocated_qty"])
        if give > req:
            return False, (
                f"Allocation {a_id}: {give} exceeds required qty {req} "
                f"for WO {orig.iloc[0]['wo_id']}."
            )
        old_give = int(orig.iloc[0]["allocated_qty"])
        new_status = (
            "ManualOverride"
            if give != old_give
            else str(erow.get("allocation_status", "Allocated"))
        )
        dl_update_rows("allocations", allocs["allocation_id"] == a_id, {
            "allocated_qty":     give,
            "allocation_status": new_status,
            "notes":             str(erow.get("notes", "")),
            "last_updated":      datetime.now(),
        })
        dl_audit("Allocation", a_id, "OVERRIDE", f"qty={old_give}", f"qty={give}", changed_by)

        # Refresh wpl from current allocations
        allocs = dl_get("allocations")
        total_for_line = int(allocs[allocs["line_id"] == line_id]["allocated_qty"].sum())
        wpl.loc[wpl["line_id"] == line_id, "received_qty"] = total_for_line

    dl_set("wo_part_lines", svc_derive_wo_part_lines(wpl))
    return True, "Overrides saved."


# ── Exception management ──────────────────────────────────────
def svc_log_exception(
    batch_id: str, part_no: str, exc_type: str, description: str, created_by: str = "System"
) -> None:
    exc_id = dl_next_id("EXC", "exceptions", "exception_id")
    dl_append("exceptions", [{
        "exception_id": exc_id,
        "batch_id":     batch_id,
        "part_no":      part_no,
        "type":         exc_type,
        "description":  description,
        "status":       "Open",
        "created_date": date.today(),
        "created_by":   created_by,
    }])
    dl_audit("Exception", exc_id, "LOGGED", "", f"{exc_type}: {description}", created_by)


def svc_close_exception(exception_id: str, changed_by: str = "System") -> None:
    excs = dl_get("exceptions")
    dl_update_rows("exceptions", excs["exception_id"] == exception_id, {"status": "Closed"})
    dl_audit("Exception", exception_id, "CLOSED", "Open", "Closed", changed_by)


# ── Responsibility transfer ───────────────────────────────────
def svc_transfer_responsibility(
    entity_type: str, entity_id: str, id_col: str,
    table: str, new_owner: str, changed_by: str = "System",
) -> None:
    rows = dl_get(table)
    old_owner = rows.loc[rows[id_col] == entity_id, "responsibility_owner"].values[0]
    dl_update_rows(table, rows[id_col] == entity_id, {
        "responsibility_owner": new_owner,
        "owner_since":          datetime.now(),
    })
    dl_audit(entity_type, entity_id, "RESPONSIBILITY_TRANSFER", old_owner, new_owner, changed_by)


# ════════════════════════════════════════════════════════════════
# DEMO DATA INITIALISER
# ════════════════════════════════════════════════════════════════
def init_data() -> None:
    today = date.today()
    b1 = "KAMB — King Abdulaziz Mechanized Brigade"
    b2 = "IMSMB — Imam Muhammad bin Saud Mechanized Brigade"

    work_orders = pd.DataFrame([
        {"wo_id":"WO-0001","brigade":b1,"workshop":"Workshop Alpha",  "created_date":today-timedelta(30),"priority":"Critical","status":"Waiting Parts"},
        {"wo_id":"WO-0002","brigade":b1,"workshop":"Workshop Alpha",  "created_date":today-timedelta(25),"priority":"High",    "status":"Waiting Parts"},
        {"wo_id":"WO-0003","brigade":b1,"workshop":"Workshop Bravo",  "created_date":today-timedelta(22),"priority":"Normal",  "status":"Waiting Parts"},
        {"wo_id":"WO-0004","brigade":b1,"workshop":"Workshop Bravo",  "created_date":today-timedelta(18),"priority":"High",    "status":"Under Maintenance"},
        {"wo_id":"WO-0005","brigade":b1,"workshop":"Workshop Bravo",  "created_date":today-timedelta(15),"priority":"Critical","status":"Waiting Parts"},
        {"wo_id":"WO-0006","brigade":b1,"workshop":"Workshop Charlie","created_date":today-timedelta(10),"priority":"Normal",  "status":"Waiting Parts"},
        {"wo_id":"WO-0007","brigade":b2,"workshop":"Workshop Alpha",  "created_date":today-timedelta(28),"priority":"Critical","status":"Waiting Parts"},
        {"wo_id":"WO-0008","brigade":b2,"workshop":"Workshop Alpha",  "created_date":today-timedelta(20),"priority":"High",    "status":"Waiting Parts"},
        {"wo_id":"WO-0009","brigade":b2,"workshop":"Workshop Bravo",  "created_date":today-timedelta(14),"priority":"Normal",  "status":"Waiting Parts"},
        {"wo_id":"WO-0010","brigade":b2,"workshop":"Workshop Charlie","created_date":today-timedelta(7), "priority":"High",    "status":"Closed"},
    ])

    raw_lines = [
        {"line_id":"LN-0001","wo_id":"WO-0001","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":20,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0002","wo_id":"WO-0001","part_no":"7161360160",  "part_desc":"BRAKE PAD",            "required_qty":8, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0003","wo_id":"WO-0002","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":15,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0004","wo_id":"WO-0002","part_no":"000000014242","part_desc":"FILTER ELEMENT,FLUID", "required_qty":5, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0005","wo_id":"WO-0003","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":12,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0006","wo_id":"WO-0003","part_no":"424316-0290", "part_desc":"BRAKE DISC",           "required_qty":10,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0007","wo_id":"WO-0004","part_no":"000000014242","part_desc":"FILTER ELEMENT,FLUID", "required_qty":4, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0008","wo_id":"WO-0005","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":18,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0009","wo_id":"WO-0005","part_no":"000000051836","part_desc":"Coolant Hose HVAC",    "required_qty":3, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0010","wo_id":"WO-0006","part_no":"WP9757-03",   "part_desc":"PUMP,WATER",           "required_qty":8, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0011","wo_id":"WO-0006","part_no":"000000016958","part_desc":"BELT,V",               "required_qty":6, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0012","wo_id":"WO-0007","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":20,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0013","wo_id":"WO-0007","part_no":"000000016958","part_desc":"BELT,V",               "required_qty":4, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0014","wo_id":"WO-0008","part_no":"7161360160",  "part_desc":"BRAKE PAD",            "required_qty":8, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0015","wo_id":"WO-0008","part_no":"424316-0290", "part_desc":"BRAKE DISC",           "required_qty":6, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0016","wo_id":"WO-0009","part_no":"1457429180",  "part_desc":"OIL FILTER",          "required_qty":15,"allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0017","wo_id":"WO-0009","part_no":"000000051836","part_desc":"Coolant Hose HVAC",    "required_qty":5, "allocated_qty":0,"received_qty":0},
        {"line_id":"LN-0018","wo_id":"WO-0010","part_no":"000000029761","part_desc":"PUMP,FUEL,ELECTRICAL", "required_qty":8, "allocated_qty":0,"received_qty":8},
    ]
    wo_part_lines = svc_derive_wo_part_lines(pd.DataFrame(raw_lines))

    st.session_state.work_orders  = work_orders
    st.session_state.wo_part_lines = wo_part_lines
    st.session_state.batches = pd.DataFrame(columns=[
        "batch_id","brigade","created_by","created_date","approval_ref",
        "batch_status","responsibility_owner","owner_since",
    ])
    st.session_state.batch_lines = pd.DataFrame(columns=[
        "batch_line_id","batch_id","part_no","part_desc","total_required_qty",
        "vendor","po_numbers","ordered_qty","received_qty","expected_delivery_date",
    ])
    st.session_state.allocations = pd.DataFrame(columns=[
        "allocation_id","batch_line_id","wo_id","line_id","allocated_qty",
        "allocation_status","last_updated","notes","responsibility_owner","owner_since",
    ])
    st.session_state.audit_log = pd.DataFrame(columns=[
        "audit_id","entity_type","entity_id","action",
        "old_value","new_value","changed_by","timestamp",
    ])
    st.session_state.exceptions = pd.DataFrame(columns=[
        "exception_id","batch_id","part_no","type",
        "description","status","created_date","created_by",
    ])
    st.session_state.config = {
        "allocation_mode": "Priority First then FIFO",
        "current_user":    "Demo User",
    }


def maybe_init() -> None:
    required = [
        "work_orders","wo_part_lines","batches","batch_lines",
        "allocations","audit_log","exceptions","config",
    ]
    if any(k not in st.session_state for k in required):
        init_data()
        return
    # Stale brigade names check (session loaded before CSV was present)
    wo_brigades = set(st.session_state.work_orders["brigade"].unique())
    if wo_brigades and not wo_brigades.intersection(set(BRIGADES)):
        for key in required + ["cb_wo_sel"]:
            st.session_state.pop(key, None)
        init_data()


# ════════════════════════════════════════════════════════════════
# UI HELPERS
# ════════════════════════════════════════════════════════════════
def current_user() -> str:
    return dl_get_config().get("current_user", "Demo User")


def kpi(label: str, value, color: str = "blue", delta: str = "") -> None:
    cls = {"red": " red", "amber": " amber", "green": " green"}.get(color, "")
    d = f'<div class="kpi-label">{delta}</div>' if delta else ""
    st.markdown(
        f'<div class="kpi-card{cls}"><div class="kpi-value">{value}</div>'
        f'<div class="kpi-label">{label}</div>{d}</div>',
        unsafe_allow_html=True,
    )


def enrich_with_catalogue(df: pd.DataFrame) -> pd.DataFrame:
    if PARTS_DF.empty:
        return df
    cat = PARTS_DF[["MNGPartNumber","UnitOfMeasure","PlatformVehicleType",
                     "Supplier","LeadTimeDays"]].rename(columns={"MNGPartNumber":"part_no"})
    return df.merge(cat, on="part_no", how="left")


# ════════════════════════════════════════════════════════════════
# ══ UI LAYER — SIDEBAR
# ════════════════════════════════════════════════════════════════
def sidebar() -> str:
    with st.sidebar:
        st.markdown("## 🔧 IMSS Batch Mgmt")
        st.markdown("---")
        page = st.radio("Navigation", [
            "📋 Work Orders",
            "➕ Create Batch",
            "📦 Procurement Updates",
            "🗂️ Allocation & Packing",
            "📊 Dashboards",
            "⚠️ Exceptions",
            "🧾 Audit Log",
            "⚙️ Settings",
        ], label_visibility="collapsed")
        st.markdown("---")
        if st.button("🔄 Reset Demo Data", use_container_width=True):
            for key in ["work_orders","wo_part_lines","batches","batch_lines",
                        "allocations","audit_log","exceptions","config","cb_wo_sel"]:
                st.session_state.pop(key, None)
            st.success("Demo data reset.")
            st.rerun()
        st.markdown("---")
        cfg = dl_get_config()
        open_exc = len(dl_get("exceptions")[dl_get("exceptions")["status"] == "Open"]) \
            if not dl_get("exceptions").empty else 0
        st.markdown(
            f"<div style='font-size:.72rem;color:#5a6080'>"
            f"👤 <b style='color:#4f8ef7'>{cfg.get('current_user','Demo User')}</b><br>"
            f"⚙️ <b style='color:#4f8ef7'>{cfg.get('allocation_mode','–').split()[0]}</b> mode<br>"
            f"📦 Parts: <b style='color:#4f8ef7'>{len(PARTS_DF):,}</b><br>"
            f"🏢 Brigades: <b style='color:#4f8ef7'>{len(BRIGADES)}</b><br>"
            f"⚠️ Open Exc: <b style='color:{'#f87171' if open_exc else '#4f8ef7'}'>{open_exc}</b>"
            f"</div>",
            unsafe_allow_html=True,
        )
    return page


# ════════════════════════════════════════════════════════════════
# ══ PAGE 1: WORK ORDERS
# ════════════════════════════════════════════════════════════════
def page_work_orders() -> None:
    st.markdown('<div class="section-header">📋 Work Orders</div>', unsafe_allow_html=True)
    tab_wo, tab_cat = st.tabs(["Work Orders", "🔍 Parts Catalogue"])

    with tab_wo:
        wo  = dl_get("work_orders").copy()
        wpl = dl_get("wo_part_lines").copy()

        c1, c2, c3, c4 = st.columns(4)
        with c1: f_brig = st.selectbox("Brigade",  ["All"] + BRIGADES)
        with c2: f_ws   = st.selectbox("Workshop", ["All"] + sorted(wo["workshop"].unique()))
        with c3: f_stat = st.selectbox("Status",   ["All"] + sorted(wo["status"].unique()))
        with c4: f_pri  = st.selectbox("Priority", ["All","Critical","High","Normal"])
        d1, d2 = st.columns(2)
        with d1: f_from = st.date_input("From", date.today() - timedelta(90))
        with d2: f_to   = st.date_input("To",   date.today())

        m = pd.Series([True] * len(wo))
        if f_brig != "All": m &= wo["brigade"]  == f_brig
        if f_ws   != "All": m &= wo["workshop"] == f_ws
        if f_stat != "All": m &= wo["status"]   == f_stat
        if f_pri  != "All": m &= wo["priority"] == f_pri
        m &= (wo["created_date"] >= f_from) & (wo["created_date"] <= f_to)
        filtered = wo[m].reset_index(drop=True)

        st.markdown(f"**{len(filtered)} work orders**")
        st.dataframe(filtered, use_container_width=True, hide_index=True)

        st.markdown("#### Part Lines Drill-Down")
        for _, row in filtered.iterrows():
            lines = wpl[wpl["wo_id"] == row["wo_id"]].copy()
            age = (date.today() - row["created_date"]).days
            age_flag = " ⚠️ >30 days" if age > 30 else ""
            with st.expander(
                f"**{row['wo_id']}** | {row['workshop']} | {row['priority']} | "
                f"{row['status']} | Age: {age}d{age_flag}"
            ):
                show = ["line_id","part_no","part_desc","required_qty",
                        "received_qty","outstanding_qty","line_status"]
                enriched = enrich_with_catalogue(lines)
                extra = [c for c in ["UnitOfMeasure","PlatformVehicleType",
                                     "Supplier","LeadTimeDays"] if c in enriched.columns]
                st.dataframe(enriched[show + extra], use_container_width=True, hide_index=True)

    with tab_cat:
        st.markdown("#### Parts Catalogue Search")
        if PARTS_DF.empty:
            st.warning("Parts catalogue CSV not found alongside app.py.")
            return
        st.caption(f"**{len(PARTS_DF):,} active parts** loaded.")
        s1, s2, s3 = st.columns(3)
        with s1: q    = st.text_input("Search part no / description")
        with s2: plat = st.selectbox("Platform", ["All"] + sorted(PARTS_DF["PlatformVehicleType"].dropna().unique()))
        with s3: uom  = st.selectbox("UoM", ["All"] + sorted(PARTS_DF["UnitOfMeasure"].dropna().unique()))
        res = PARTS_DF.copy()
        if q.strip():
            qup = q.strip().upper()
            res = res[
                res["MNGPartNumber"].str.upper().str.contains(qup, na=False)
                | res["DescriptionEn"].str.upper().str.contains(qup, na=False)
            ]
        if plat != "All": res = res[res["PlatformVehicleType"] == plat]
        if uom  != "All": res = res[res["UnitOfMeasure"] == uom]
        st.markdown(f"**{len(res):,} found**")
        dcols = ["MNGPartNumber","DescriptionEn","UnitOfMeasure","PlatformVehicleType",
                 "Supplier","LeadTimeDays","Criticality","NSN","OEMPartNumber"]
        dcols = [c for c in dcols if c in res.columns]
        st.dataframe(res[dcols].head(500), use_container_width=True, hide_index=True)
        if len(res) > 500:
            st.caption("Showing first 500 — refine search to narrow results.")


# ════════════════════════════════════════════════════════════════
# ══ PAGE 2: CREATE BATCH
# ════════════════════════════════════════════════════════════════
def page_create_batch() -> None:
    st.markdown('<div class="section-header">➕ Create Batch</div>', unsafe_allow_html=True)

    wo  = dl_get("work_orders")
    wpl = dl_get("wo_part_lines")

    # Locked line IDs (already in an active batch)
    active_s = {"Draft","Subm to Procurement","Under Procurement","Partially Received"}
    active_bids = dl_get("batches")[dl_get("batches")["batch_status"].isin(active_s)]["batch_id"].tolist()
    locked = dl_get("allocations")[
        dl_get("allocations")["batch_line_id"].isin(
            dl_get("batch_lines")[dl_get("batch_lines")["batch_id"].isin(active_bids)]["batch_line_id"]
        )
    ]["line_id"].tolist()

    elig_brigades = [b for b in BRIGADES
                     if b in set(wo[wo["status"] == "Waiting Parts"]["brigade"])]
    if not elig_brigades:
        st.warning("No 'Waiting Parts' Work Orders exist across any brigade.")
        elig_brigades = BRIGADES

    brigade = st.selectbox("Select Brigade", elig_brigades,
                           help="Only brigades with eligible WOs are shown.")

    elig_wo = wo[(wo["brigade"] == brigade) & (wo["status"] == "Waiting Parts")].copy()
    if elig_wo.empty:
        st.warning(f"No eligible WOs for {brigade}.")
        _render_submit_drafts(brigade)
        return

    elig_lines = (
        wpl[
            (wpl["wo_id"].isin(elig_wo["wo_id"]))
            & (~wpl["line_id"].isin(locked))
            & (wpl["line_status"] != "Ready")
        ]
        .merge(wo[["wo_id","priority","created_date","workshop"]], on="wo_id", how="left")
    )

    summary = (
        elig_lines
        .groupby(["wo_id","priority","created_date","workshop"], as_index=False)
        .agg(part_lines=("line_id","count"), total_outstanding=("outstanding_qty","sum"))
        .sort_values(["priority","created_date"],
                     key=lambda c: c.map(PRIORITY_ORDER) if c.name == "priority" else c)
    )

    st.markdown(f"#### Select Work Orders — **{brigade}**")
    st.caption("Tick WOs to include. All eligible part lines per WO will be added.")

    if "cb_wo_sel" not in st.session_state:
        st.session_state.cb_wo_sel = {}

    p_colors = {"Critical":"#f87171","High":"#fbbf24","Normal":"#94a3b8"}
    hdr = st.columns([.5,1.5,1.5,1.5,1.5,1.2,1.2])
    for col, lbl in zip(hdr, ["✓","WO ID","Workshop","Priority","Created","Lines","Outstanding"]):
        col.markdown(f"**{lbl}**")
    st.markdown("<hr style='margin:4px 0 8px 0;border-color:#2d3150'>", unsafe_allow_html=True)

    for i, (_, row) in enumerate(summary.iterrows()):
        wid = row["wo_id"]
        key = f"cb_{wid}"
        cols = st.columns([.5,1.5,1.5,1.5,1.5,1.2,1.2])
        checked = cols[0].checkbox("", value=st.session_state.cb_wo_sel.get(key, i < 3),
                                    key=key, label_visibility="collapsed")
        st.session_state.cb_wo_sel[key] = checked
        cols[1].markdown(f"`{wid}`")
        cols[2].markdown(row["workshop"])
        c = p_colors.get(row["priority"], "#aaa")
        cols[3].markdown(f"<span style='color:{c};font-weight:600'>{row['priority']}</span>",
                         unsafe_allow_html=True)
        cols[4].markdown(str(row["created_date"]))
        cols[5].markdown(str(int(row["part_lines"])))
        cols[6].markdown(f"**{int(row['total_outstanding'])}**")

    selected_wo = [r["wo_id"] for _, r in summary.iterrows()
                   if st.session_state.cb_wo_sel.get(f"cb_{r['wo_id']}", False)]
    sel_lines = elig_lines[elig_lines["wo_id"].isin(selected_wo)].copy()

    if sel_lines.empty:
        st.info("Tick at least one Work Order to continue.")
        _render_submit_drafts(brigade)
        return

    with st.expander(f"📋 {len(sel_lines)} part lines included"):
        st.dataframe(sel_lines[["line_id","wo_id","part_no","part_desc","outstanding_qty"]],
                     use_container_width=True, hide_index=True)

    agg = (
        sel_lines.groupby(["part_no","part_desc"], as_index=False)["outstanding_qty"]
        .sum().rename(columns={"outstanding_qty":"total_required_qty"})
    )
    st.markdown("**Aggregated Batch Lines:**")
    st.dataframe(agg, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1: approval_ref = st.text_input("Approval Reference", placeholder="APPREF-2024-001")
    with c2: created_by   = st.text_input("Created By", value=current_user())
    submit_imm = st.checkbox("Submit to Procurement immediately")

    if st.button("✅ Create Batch", type="primary"):
        if not approval_ref.strip():
            st.error("Approval Reference is required.")
            return
        ok, result = svc_create_batch(
            brigade, selected_wo, approval_ref.strip(), created_by.strip(), submit_imm
        )
        if ok:
            status_label = "Subm to Procurement" if submit_imm else "Draft"
            st.success(f"✅ Batch **{result}** created — Brigade: {brigade} — Status: **{status_label}**")
            for k in list(st.session_state.cb_wo_sel.keys()):
                del st.session_state.cb_wo_sel[k]
            st.rerun()
        else:
            st.error(f"❌ {result}")

    _render_submit_drafts(brigade)


def _render_submit_drafts(brigade: str) -> None:
    st.markdown("---")
    st.markdown("#### 📤 Submit Draft Batches to Procurement")
    drafts = dl_get("batches")[
        (dl_get("batches")["batch_status"] == "Draft")
        & (dl_get("batches")["brigade"] == brigade)
    ]
    if drafts.empty:
        st.info("No draft batches for this brigade.")
        return
    st.dataframe(drafts, use_container_width=True, hide_index=True)
    bid = st.selectbox("Select Draft", drafts["batch_id"].tolist(), key="submit_sel")
    if st.button("Submit to Procurement", key="submit_btn"):
        ok, msg = svc_transition_batch(bid, "Subm to Procurement", current_user())
        (st.success if ok else st.error)(msg)
        if ok:
            st.rerun()


# ════════════════════════════════════════════════════════════════
# ══ PAGE 3: PROCUREMENT UPDATES
# ════════════════════════════════════════════════════════════════
def page_procurement_updates() -> None:
    st.markdown('<div class="section-header">📦 Procurement Updates</div>', unsafe_allow_html=True)

    valid_s = ["Subm to Procurement","Under Procurement","Partially Received","Fully Received"]
    elig = dl_get("batches")[dl_get("batches")["batch_status"].isin(valid_s)]
    if elig.empty:
        st.warning("No batches available. Create and submit a batch first.")
        return

    sel = st.selectbox("Select Batch", elig["batch_id"].tolist())
    info = dl_get("batches")[dl_get("batches")["batch_id"] == sel].iloc[0]

    mc = st.columns(4)
    mc[0].metric("Brigade", info["brigade"].split("—")[0].strip() if "—" in info["brigade"] else info["brigade"])
    mc[1].metric("Status",  info["batch_status"])
    mc[2].metric("Approval", info["approval_ref"])
    mc[3].metric("Owner",    info.get("responsibility_owner", "–"))

    # ── State machine transitions ─────────────────────────────
    st.markdown("#### 🔄 Batch Status")
    allowed = BATCH_TRANSITIONS.get(info["batch_status"], [])
    if allowed:
        tc = st.columns(len(allowed) + 1)
        for i, ns in enumerate(allowed):
            if tc[i].button(f"→ {ns}", key=f"trans_{ns}"):
                try:
                    if ns == "Fully Received":
                        svc_validate_fully_received(sel)
                    if ns == "Closed":
                        # Require explicit confirm
                        st.session_state["_confirm_close"] = sel
                    else:
                        ok, msg = svc_transition_batch(sel, ns, current_user())
                        (st.success if ok else st.error)(msg)
                        if ok:
                            st.rerun()
                except ValidationError as exc:
                    st.error(str(exc))

    if st.session_state.get("_confirm_close") == sel:
        st.warning("⚠️ Closing is irreversible. Confirm?")
        cc1, cc2 = st.columns(2)
        if cc1.button("✅ Yes, Close Batch", key="yes_close"):
            ok, msg = svc_transition_batch(sel, "Closed", current_user())
            (st.success if ok else st.error)(msg)
            st.session_state.pop("_confirm_close", None)
            if ok:
                st.rerun()
        if cc2.button("Cancel", key="no_close"):
            st.session_state.pop("_confirm_close", None)
            st.rerun()
    else:
        st.info(f"Allowed transitions: {allowed}" if allowed else "No further transitions.")

    # ── Responsibility transfer ───────────────────────────────
    with st.expander("🔁 Transfer Responsibility"):
        new_owner = st.selectbox("New Owner", RESPONSIBILITY_OWNERS, key="proc_owner")
        if st.button("Transfer", key="proc_transfer"):
            svc_transfer_responsibility("Batch", sel, "batch_id", "batches",
                                        new_owner, current_user())
            st.success(f"Responsibility → {new_owner}")
            st.rerun()

    # ── Batch lines editor ────────────────────────────────────
    st.markdown("#### Batch Lines — Procurement Details")
    bl = dl_get("batch_lines")[dl_get("batch_lines")["batch_id"] == sel].copy()
    if bl.empty:
        st.info("No batch lines.")
        return

    enriched = enrich_with_catalogue(bl[["batch_line_id","part_no","part_desc",
                                         "total_required_qty","received_qty"]].copy())
    cat_c = [c for c in ["PlatformVehicleType","Supplier","LeadTimeDays"] if c in enriched.columns]
    if cat_c:
        with st.expander("📖 Catalogue Details"):
            st.dataframe(enriched[["part_no","part_desc"] + cat_c],
                         use_container_width=True, hide_index=True)

    edited = st.data_editor(
        bl[["batch_line_id","part_no","part_desc","total_required_qty",
            "vendor","po_numbers","ordered_qty","received_qty","expected_delivery_date"]],
        use_container_width=True, hide_index=True,
        disabled=["batch_line_id","part_no","part_desc","total_required_qty"],
        column_config={
            "received_qty":           st.column_config.NumberColumn("Received Qty", min_value=0),
            "ordered_qty":            st.column_config.NumberColumn("Ordered Qty",  min_value=0),
            "expected_delivery_date": st.column_config.DateColumn("Expected Delivery"),
        },
        key=f"proc_{sel}",
    )

    if st.button("💾 Save Procurement Updates", type="primary"):
        errors = []
        for _, erow in edited.iterrows():
            ok, msg = svc_update_procurement_line(
                str(erow["batch_line_id"]),
                str(erow.get("vendor","")),
                str(erow.get("po_numbers","")),
                int(erow.get("ordered_qty", 0)),
                int(erow.get("received_qty", 0)),
                erow.get("expected_delivery_date"),
                current_user(),
            )
            if not ok:
                errors.append(f"{erow['batch_line_id']}: {msg}")
        if errors:
            for e in errors:
                st.error(f"❌ {e}")
        else:
            st.success("✅ Procurement details saved. Allocation engine re-run.")
            st.rerun()

    # ── Allocation summary ────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 📊 Allocation Summary")
    allocs = dl_get("allocations")[
        dl_get("allocations")["batch_line_id"].isin(bl["batch_line_id"])
    ].copy()
    if not allocs.empty:
        allocs = (
            allocs
            .merge(dl_get("work_orders")[["wo_id","priority"]], on="wo_id", how="left")
            .merge(dl_get("wo_part_lines")[["line_id","part_no","required_qty",
                                            "outstanding_qty","line_status"]],
                   on="line_id", how="left")
        )
        st.dataframe(
            allocs[["batch_line_id","wo_id","priority","part_no",
                    "required_qty","allocated_qty","outstanding_qty","allocation_status"]],
            use_container_width=True, hide_index=True,
        )


# ════════════════════════════════════════════════════════════════
# ══ PAGE 4: ALLOCATION & PACKING
# ════════════════════════════════════════════════════════════════
def page_allocation_packing() -> None:
    st.markdown('<div class="section-header">🗂️ Allocation & Packing</div>', unsafe_allow_html=True)

    batches = dl_get("batches")
    if batches.empty:
        st.warning("No batches exist.")
        return

    sel_batch = st.selectbox("Select Batch", batches["batch_id"].tolist())
    bl = dl_get("batch_lines")[dl_get("batch_lines")["batch_id"] == sel_batch]
    if bl.empty:
        st.info("No batch lines.")
        return

    sel_bl = st.selectbox("Select Batch Line", bl["batch_line_id"].tolist())
    bl_info  = bl[bl["batch_line_id"] == sel_bl].iloc[0]
    total_r  = int(bl_info["received_qty"])
    total_req = int(bl_info["total_required_qty"])
    pct = round(100 * total_r / total_req) if total_req else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Part No",    bl_info["part_no"])
    m2.metric("Required",   total_req)
    m3.metric("Received",   total_r)
    m4.metric("Allocated %", f"{pct}%",
              delta="✅ Full" if pct >= 100 else (f"⚠️ {100-pct}% outstanding" if pct > 0 else "❌ None"))

    allocs = dl_get("allocations")[
        dl_get("allocations")["batch_line_id"] == sel_bl
    ].copy()
    if allocs.empty:
        st.info("No allocations for this batch line.")
        return

    allocs = (
        allocs
        .merge(dl_get("work_orders")[["wo_id","priority","created_date","workshop"]],
               on="wo_id", how="left")
        .merge(dl_get("wo_part_lines")[["line_id","required_qty","received_qty","outstanding_qty"]],
               on="line_id", how="left")
    )

    rc1, rc2 = st.columns([3, 1])
    rc1.markdown(
        f"**Manual Override** — max allocatable: **{total_r}** units  \n"
        f"_Mode: {dl_get_config().get('allocation_mode','–')}_",
        help="Manual overrides are flagged as ManualOverride and protected from auto-reduction.",
    )
    if rc2.button("🔁 Reset to Auto", help="Clears manual overrides and re-runs allocation engine"):
        svc_reset_allocation_to_auto(sel_bl, current_user())
        st.success("Auto allocation reset complete.")
        st.rerun()

    edit_cols = ["allocation_id","wo_id","priority","created_date",
                 "required_qty","received_qty","outstanding_qty",
                 "allocated_qty","allocation_status","notes"]

    edited = st.data_editor(
        allocs[edit_cols],
        use_container_width=True, hide_index=True,
        disabled=["allocation_id","wo_id","priority","created_date",
                  "required_qty","received_qty","outstanding_qty"],
        column_config={
            "allocated_qty": st.column_config.NumberColumn("Allocated Qty", min_value=0),
            "allocation_status": st.column_config.SelectboxColumn("Status", options=[
                "Allocated","ManualOverride","Packed","Collected","DeliveredConfirmed",
            ]),
        },
        key=f"alloc_{sel_bl}",
    )

    if st.button("💾 Save Overrides", type="primary"):
        ok, msg = svc_apply_allocation_override(edited, sel_bl, current_user())
        (st.success if ok else st.error)(("✅ " if ok else "❌ ") + msg)
        if ok:
            st.rerun()

    with st.expander("🔁 Transfer Allocation Responsibility"):
        a_opts = allocs["allocation_id"].tolist()
        a_sel  = st.selectbox("Allocation", a_opts, key="a_own_sel")
        a_own  = st.selectbox("New Owner", RESPONSIBILITY_OWNERS, key="a_own_new")
        if st.button("Transfer", key="a_own_btn"):
            svc_transfer_responsibility("Allocation", a_sel, "allocation_id",
                                        "allocations", a_own, current_user())
            st.success(f"Allocation responsibility → {a_own}")
            st.rerun()

    # ── Packing lists ─────────────────────────────────────────
    st.markdown("---")
    t1, t2 = st.tabs(["Per-WO Packing List", "Collection Manifest"])
    all_a = (
        dl_get("allocations")[
            dl_get("allocations")["batch_line_id"].isin(bl["batch_line_id"])
        ]
        .merge(dl_get("batch_lines")[["batch_line_id","part_no","part_desc"]],
               on="batch_line_id", how="left")
    )
    with t1:
        wo_opts = all_a["wo_id"].unique().tolist()
        sel_wo  = st.selectbox("Work Order", wo_opts, key="pack_wo")
        pack = all_a[all_a["wo_id"] == sel_wo]
        st.markdown(f"**Packing List — {sel_wo} / {sel_batch}**")
        st.dataframe(pack[["part_no","part_desc","allocated_qty","allocation_status"]],
                     use_container_width=True, hide_index=True)
    with t2:
        manifest = (
            all_a[["wo_id","part_no","part_desc","allocated_qty","allocation_status"]]
            .sort_values(["wo_id","part_no"])
        )
        st.dataframe(manifest, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════
# ══ PAGE 5: DASHBOARDS
# ════════════════════════════════════════════════════════════════
def page_dashboards() -> None:
    st.markdown('<div class="section-header">📊 Dashboards</div>', unsafe_allow_html=True)

    wo       = dl_get("work_orders")
    wpl      = dl_get("wo_part_lines")
    batches  = dl_get("batches")
    bl       = dl_get("batch_lines")
    exc      = dl_get("exceptions")

    open_wo  = wo[wo["status"] == "Waiting Parts"]
    open_exc = int(len(exc[exc["status"] == "Open"]) if not exc.empty else 0)

    # Top KPIs
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: kpi("Open WOs",         len(open_wo),  "red"   if len(open_wo) > 5 else "blue")
    with k2: kpi("Total Batches",    len(batches))
    with k3: kpi("Parts Ready",      len(wpl[wpl["line_status"] == "Ready"]),   "green")
    with k4: kpi("Parts Waiting",    len(wpl[wpl["line_status"] == "Waiting"]), "amber")
    with k5: kpi("Open Exceptions",  open_exc,      "red"   if open_exc > 0 else "green")

    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs(["Operational", "Batch KPIs", "Aging & Delays", "Responsibility"])

    # ── Tab 1: Operational ────────────────────────────────────
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Open WOs by Workshop")
            st.dataframe(
                open_wo.groupby("workshop").size().reset_index(name="Count"),
                use_container_width=True, hide_index=True,
            )
            st.markdown("#### Avg WO Age by Workshop (days)")
            wo2 = wo.copy()
            wo2["age_days"] = (date.today() - wo2["created_date"]).apply(lambda x: x.days)
            age_ws = wo2.groupby("workshop")["age_days"].mean().round(1).reset_index()
            age_ws.columns = ["Workshop","Avg Age (days)"]
            st.dataframe(age_ws, use_container_width=True, hide_index=True)
        with c2:
            st.markdown("#### Part Line Status")
            ls = wpl["line_status"].value_counts().reset_index()
            ls.columns = ["Status","Count"]
            st.dataframe(ls, use_container_width=True, hide_index=True)
            st.markdown("#### Priority Distribution (open WOs)")
            pd_dist = (
                open_wo.groupby("priority").size()
                .reindex(["Critical","High","Normal"], fill_value=0)
                .reset_index()
            )
            pd_dist.columns = ["Priority","Count"]
            st.dataframe(pd_dist, use_container_width=True, hide_index=True)

    # ── Tab 2: Batch KPIs ─────────────────────────────────────
    with tab2:
        if batches.empty:
            st.info("No batches created yet.")
        else:
            st.markdown("#### Batches by Status")
            bs = batches["batch_status"].value_counts().reset_index()
            bs.columns = ["Status","Count"]
            st.dataframe(bs, use_container_width=True, hide_index=True)

            st.markdown("#### Procurement Cycle Time")
            bc = batches.copy()
            bc["cycle_days"] = (
                pd.Timestamp.today() - pd.to_datetime(bc["created_date"])
            ).dt.days
            st.dataframe(
                bc[["batch_id","brigade","batch_status","cycle_days","responsibility_owner"]],
                use_container_width=True, hide_index=True,
            )

            if not bl.empty:
                st.markdown("#### Allocation Efficiency (received / required)")
                eff = (
                    bl.groupby("batch_id")
                    .apply(lambda g: pd.Series({
                        "required":    int(g["total_required_qty"].sum()),
                        "received":    int(g["received_qty"].sum()),
                    }))
                    .reset_index()
                )
                eff["efficiency_pct"] = (
                    eff["received"] / eff["required"] * 100
                ).round(1).fillna(0)
                st.dataframe(eff, use_container_width=True, hide_index=True)

    # ── Tab 3: Aging & Delays ─────────────────────────────────
    with tab3:
        wo3 = wo.copy()
        wo3["age_days"] = (date.today() - wo3["created_date"]).apply(lambda x: x.days)

        st.markdown("#### WO Aging Buckets")
        wo3["bucket"] = pd.cut(
            wo3["age_days"], bins=[0, 30, 60, 99999],
            labels=["0–30 days","31–60 days","60+ days"],
        )
        bkt = wo3.groupby("bucket").size().reset_index(name="Count")
        st.dataframe(bkt, use_container_width=True, hide_index=True)

        st.markdown("#### Top 10 Delayed Work Orders")
        top10 = (
            wo3.sort_values("age_days", ascending=False)
            .head(10)[["wo_id","brigade","workshop","priority","status","age_days"]]
        )
        st.dataframe(top10, use_container_width=True, hide_index=True)

        if not batches.empty:
            st.markdown("#### Average Days in Under-Procurement State")
            up = batches[batches["batch_status"].isin(["Under Procurement","Partially Received"])].copy()
            if not up.empty:
                up["days"] = (pd.Timestamp.today() - pd.to_datetime(up["created_date"])).dt.days
                st.dataframe(up[["batch_id","brigade","batch_status","days"]],
                             use_container_width=True, hide_index=True)
            else:
                st.info("No batches currently under procurement.")

    # ── Tab 4: Responsibility ─────────────────────────────────
    with tab4:
        if batches.empty:
            st.info("No batches.")
        else:
            bc4 = batches.copy()
            bc4["days_with_owner"] = bc4["owner_since"].apply(
                lambda x: (datetime.now() - x).days if pd.notna(x) else 0
            )
            st.markdown("#### Batch Responsibility Overview")
            st.dataframe(
                bc4[["batch_id","batch_status","responsibility_owner","days_with_owner"]],
                use_container_width=True, hide_index=True,
            )
            st.markdown("#### Batches per Owner")
            owner_counts = bc4.groupby("responsibility_owner").size().reset_index(name="Batches")
            st.dataframe(owner_counts, use_container_width=True, hide_index=True)


# ════════════════════════════════════════════════════════════════
# ══ PAGE 6: EXCEPTIONS
# ════════════════════════════════════════════════════════════════
def page_exceptions() -> None:
    st.markdown('<div class="section-header">⚠️ Exception Management</div>', unsafe_allow_html=True)

    batches = dl_get("batches")
    exc     = dl_get("exceptions")

    st.markdown("#### Log New Exception")
    ec1, ec2, ec3 = st.columns(3)
    with ec1: exc_batch = st.selectbox("Batch", [""] + batches["batch_id"].tolist() if not batches.empty else [""])
    with ec2: exc_part  = st.text_input("Part No")
    with ec3: exc_type  = st.selectbox("Exception Type", EXCEPTION_TYPES)
    exc_desc = st.text_area("Description", placeholder="Describe the issue…")

    if st.button("⚠️ Log Exception", type="primary"):
        if not exc_batch or not exc_part.strip() or not exc_desc.strip():
            st.error("Batch, Part No, and Description are required.")
        else:
            svc_log_exception(exc_batch, exc_part.strip(), exc_type,
                              exc_desc.strip(), current_user())
            st.success(f"Exception logged for batch {exc_batch}.")
            st.rerun()

    st.markdown("---")
    st.markdown("#### Exception Register")
    if exc.empty:
        st.info("No exceptions logged.")
        return

    f1, f2 = st.columns(2)
    with f1: f_s = st.selectbox("Status", ["All","Open","Closed"])
    with f2: f_t = st.selectbox("Type",   ["All"] + EXCEPTION_TYPES)
    view = exc.copy()
    if f_s != "All": view = view[view["status"] == f_s]
    if f_t != "All": view = view[view["type"]   == f_t]

    for _, row in view.iterrows():
        icon = "🔴" if row["status"] == "Open" else "✅"
        with st.expander(
            f"{icon} {row['exception_id']} | {row['type']} | "
            f"{row['batch_id']} | {row['part_no']}"
        ):
            st.markdown(f"**Description:** {row['description']}")
            st.markdown(f"**Status:** {row['status']} | **Created:** {row['created_date']} | **By:** {row.get('created_by','–')}")
            if row["status"] == "Open":
                if st.button("✅ Close", key=f"close_{row['exception_id']}"):
                    svc_close_exception(row["exception_id"], current_user())
                    st.success("Exception closed.")
                    st.rerun()


# ════════════════════════════════════════════════════════════════
# ══ PAGE 7: AUDIT LOG
# ════════════════════════════════════════════════════════════════
def page_audit_log() -> None:
    st.markdown('<div class="section-header">🧾 Audit Log</div>', unsafe_allow_html=True)

    log = dl_get("audit_log")
    if log.empty:
        st.info("No audit entries yet. Save any record to generate entries.")
        return

    c1, c2, c3 = st.columns(3)
    with c1: f_ent = st.selectbox("Entity Type", ["All"] + sorted(log["entity_type"].unique()))
    with c2: f_act = st.selectbox("Action",      ["All"] + sorted(log["action"].unique()))
    with c3: f_usr = st.selectbox("Changed By",  ["All"] + sorted(log["changed_by"].unique()))

    view = log.copy()
    if f_ent != "All": view = view[view["entity_type"] == f_ent]
    if f_act != "All": view = view[view["action"]      == f_act]
    if f_usr != "All": view = view[view["changed_by"]  == f_usr]
    view = view.sort_values("timestamp", ascending=False)

    st.markdown(f"**{len(view)} audit entries**")
    st.dataframe(
        view[["timestamp","entity_type","entity_id","action","old_value","new_value","changed_by"]],
        use_container_width=True, hide_index=True,
    )


# ════════════════════════════════════════════════════════════════
# ══ PAGE 8: SETTINGS
# ════════════════════════════════════════════════════════════════
def page_settings() -> None:
    st.markdown('<div class="section-header">⚙️ Settings</div>', unsafe_allow_html=True)

    cfg = dl_get_config()

    st.markdown("#### User")
    new_user = st.text_input("Current User", value=cfg.get("current_user","Demo User"))
    if st.button("Save User"):
        dl_set_config("current_user", new_user.strip())
        st.success(f"User set to '{new_user.strip()}'.")

    st.markdown("---")
    st.markdown("#### Allocation Engine Mode")
    st.info(
        "**Priority First then FIFO** — Critical → High → Normal, then oldest first.  \n"
        "**FIFO** — Oldest WO first regardless of priority.  \n"
        "**Manual Only** — Engine is disabled; allocate everything manually."
    )
    cur = cfg.get("allocation_mode","Priority First then FIFO")
    idx = ALLOCATION_MODES.index(cur) if cur in ALLOCATION_MODES else 0
    new_mode = st.radio("Allocation Mode", ALLOCATION_MODES, index=idx)
    if st.button("Save Allocation Mode"):
        dl_set_config("allocation_mode", new_mode)
        dl_audit("Config","allocation_mode","SETTING_CHANGE", cur, new_mode, current_user())
        st.success(f"Allocation mode → **{new_mode}**")
        st.rerun()

    st.markdown("---")
    st.markdown("#### System Info")
    st.json({
        "parts_loaded":   len(PARTS_DF),
        "brigades_loaded": len(BRIGADES),
        "work_orders":    len(dl_get("work_orders")),
        "batches":        len(dl_get("batches")),
        "batch_lines":    len(dl_get("batch_lines")),
        "allocations":    len(dl_get("allocations")),
        "audit_entries":  len(dl_get("audit_log")),
        "exceptions":     len(dl_get("exceptions")),
    })


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
def main() -> None:
    maybe_init()
    page = sidebar()
    {
        "📋 Work Orders":          page_work_orders,
        "➕ Create Batch":          page_create_batch,
        "📦 Procurement Updates":  page_procurement_updates,
        "🗂️ Allocation & Packing":  page_allocation_packing,
        "📊 Dashboards":           page_dashboards,
        "⚠️ Exceptions":           page_exceptions,
        "🧾 Audit Log":            page_audit_log,
        "⚙️ Settings":             page_settings,
    }.get(page, page_work_orders)()


if __name__ == "__main__":
    main()
