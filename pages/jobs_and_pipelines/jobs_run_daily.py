import datetime as dt
import json
import os

import altair as alt
import pandas as pd
import pytz
import streamlit as st
from databricks.sdk.service.sql import Disposition, Format, StatementState
from pages.utils import make_workspace_client, COMMON_TZ, match_team_rules
from pages.settings.storage import get_cached_settings, get_cached_user_prefs

st.header("Job Runs History")

_w_settings = make_workspace_client()
_settings = get_cached_settings(_w_settings)
_global_tz = _settings["timezone"]
_teams_cfg = _settings["teams"]
_team_names = [t["name"] for t in _teams_cfg]

# Workspace options (id → display label)
_WORKSPACE_OPTIONS = {
    "36379689778622": "WS01 (Prod)",
    "6288329693138990": "WS02 (Non-Prod)",
}
_WS_LABELS = list(_WORKSPACE_OPTIONS.values())
_WS_IDS = list(_WORKSPACE_OPTIONS.keys())

# Restore filter state from URL query params on first load
if "last_run_tz" not in st.session_state:
    _qp_tz = st.query_params.get("tz", "")
    st.session_state["last_run_tz"] = _qp_tz if _qp_tz in COMMON_TZ else _global_tz

if "last_run_days" not in st.session_state:
    try:
        st.session_state["last_run_days"] = max(1, min(60, int(st.query_params.get("days", "30"))))
    except (ValueError, TypeError):
        st.session_state["last_run_days"] = 30

if "last_run_ws" not in st.session_state:
    st.session_state["last_run_ws"] = _WS_LABELS[0]  # Default to first (Prod)

def _on_tz_change():
    st.query_params["tz"] = st.session_state["last_run_tz"]

def _on_days_change():
    st.query_params["days"] = str(st.session_state["last_run_days"])

col_tz, col_ws, col_days, col_teams = st.columns([0.10, 0.12, 0.53, 0.25])
selected_tz = col_tz.selectbox(
    "Timezone", options=COMMON_TZ,
    key="last_run_tz", on_change=_on_tz_change,
)
selected_ws_label = col_ws.selectbox(
    "Workspace", options=_WS_LABELS,
    key="last_run_ws",
)
lookback_days = col_days.slider(
    "Lookback days", min_value=1, max_value=60,
    key="last_run_days", on_change=_on_days_change,
)
if "last_run_teams" not in st.session_state:
    _default_team_ids = get_cached_user_prefs(_w_settings).get("default_teams", [])
    _id_to_name = {t["id"]: t["name"] for t in _teams_cfg}
    _default_team_names = [_id_to_name[tid] for tid in _default_team_ids if tid in _id_to_name]
    st.session_state["last_run_teams"] = [n for n in _default_team_names if n in _team_names]
selected_teams = col_teams.multiselect(
    "Teams", options=_team_names,
    placeholder="All teams", key="last_run_teams",
)

tz = pytz.timezone(selected_tz)
now_local = dt.datetime.now(tz)

start_ts = (now_local - dt.timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
end_ts = now_local.strftime("%Y-%m-%d %H:%M:%S")

w = make_workspace_client()
user_w = w


# ── SQL execution helper ──────────────────────────────────────────────────────

def _get_warehouse_id(w) -> str:
    """Get SQL warehouse ID from app resource env var or auto-discover."""
    wh_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if wh_id:
        return wh_id
    # Auto-discover first available warehouse
    warehouses = list(w.warehouses.list())
    if not warehouses:
        st.error("No SQL warehouse available. Add a SQL warehouse resource to the app.")
        st.stop()
    return warehouses[0].id


def _execute_sql(w, sql: str) -> pd.DataFrame:
    """Execute SQL via SDK statement execution and return a DataFrame."""
    warehouse_id = _get_warehouse_id(w)
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    if resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"SQL query failed: {resp.status.error}")
    columns = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array if resp.result and resp.result.data_array else []
    return pd.DataFrame(rows, columns=columns)


def _parse_timestamp(ts_str, tz):
    """Parse timestamp string from statement_execution (ISO format with Z suffix)."""
    ts = pd.Timestamp(ts_str)
    # Timestamps from statement_execution are already tz-aware (UTC via Z suffix)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.to_pydatetime().astimezone(tz)


# ── Fetch data from system tables ─────────────────────────────────────────────

# Resolve selected workspace label → ID
workspace_id = _WS_IDS[_WS_LABELS.index(selected_ws_label)]

with st.spinner("Fetching data…"):
    try:
        # Get latest job definitions (non-deleted, non-pipeline)
        df_jobs = _execute_sql(w, f"""
            WITH ranked AS (
                SELECT job_id, name, tags, creator_id,
                       ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
                FROM maic.silver_system_tables.jobs
                WHERE workspace_id = '{workspace_id}'
            )
            SELECT job_id, name, tags, creator_id
            FROM ranked
            WHERE rn = 1
        """)
    except Exception as e:
        st.error(f"Failed to fetch job list: {e}")
        st.stop()

    try:
        # Get run history within lookback period (JOB_RUN only)
        df_runs = _execute_sql(w, f"""
            SELECT job_id, run_id, period_start_time, period_end_time,
                   result_state, run_duration_seconds
            FROM maic.silver_system_tables.job_run_timeline
            WHERE workspace_id = '{workspace_id}'
              AND run_type = 'JOB_RUN'
              AND period_start_time >= TIMESTAMP '{start_ts}'
              AND period_start_time <= TIMESTAMP '{end_ts}'
            ORDER BY period_start_time DESC
        """)
    except Exception as e:
        st.error(f"Failed to fetch runs: {e}")
        st.stop()

# Exclude pipeline jobs: identify jobs that have pipeline runs
# (Pipeline-wrapper jobs typically also trigger SUBMIT_RUN pipeline runs.
#  If stricter filtering is needed, check job_task_run_timeline for pipeline_task.)
pipeline_job_ids: set = set()
# NOTE: If you need pipeline exclusion, add logic here using job_task_run_timeline
# or filter by job name/tag patterns.

# Build registry: job_id → job metadata
registry = {}
for _, row in df_jobs.iterrows():
    jid = row["job_id"]
    if jid in pipeline_job_ids:
        continue
    # Parse tags from JSON string (system table returns MAP as JSON)
    tags_raw = row.get("tags")
    try:
        tags = json.loads(tags_raw) if tags_raw and tags_raw != "null" else {}
    except (json.JSONDecodeError, TypeError):
        tags = {}
    registry[jid] = {
        "name": row["name"] or f"job-{jid}",
        "tags": tags,
        "creator_id": row.get("creator_id") or "unknown",
    }

registry_id_to_name = {jid: meta["name"] for jid, meta in registry.items()}
job_to_id = {meta["name"]: jid for jid, meta in registry.items()}

# Build records from run data
records = []
job_to_running_run_id = {}

for _, run in df_runs.iterrows():
    jid = run["job_id"]
    if jid not in registry_id_to_name:
        continue

    name = registry_id_to_name[jid]
    rs = run["result_state"]  # SUCCEEDED, ERROR, CANCELLED, or None/null

    # Parse start time (timestamps arrive as ISO strings with Z suffix, already tz-aware)
    try:
        run_start = _parse_timestamp(run["period_start_time"], tz)
    except Exception:
        continue

    # Parse end time
    try:
        run_end = _parse_timestamp(run["period_end_time"], tz)
    except Exception:
        run_end = run_start

    # Duration
    dur_sec = run.get("run_duration_seconds")
    try:
        duration_min = float(dur_sec) / 60 if dur_sec and dur_sec != "null" and dur_sec != "0" else (run_end - run_start).total_seconds() / 60
    except (ValueError, TypeError):
        duration_min = (run_end - run_start).total_seconds() / 60

    # Map result_state to display status
    if rs is None or rs == "" or rs == "null":
        status = "RUNNING"
        # Track active runs for stop button
        if name not in job_to_running_run_id:
            job_to_running_run_id[name] = run["run_id"]
    elif rs == "SUCCEEDED":
        status = "SUCCESS"
    elif rs == "CANCELLED":
        status = "CANCELED"
    else:
        status = "FAILED"

    records.append({
        "job": name,
        "job_id": jid,
        "run_time": run_start,
        "duration_min": round(duration_min, 1),
        "status": status,
    })

# Team filtering
if selected_teams:
    matched_ids = {
        jid for jid, meta in registry.items()
        if any(
            m in selected_teams
            for m in match_team_rules(
                meta["name"], meta["creator_id"], _teams_cfg, tags=meta["tags"],
            )
        )
    }
    records = [r for r in records if r.get("job_id") in matched_ids]

if not records:
    st.info(f"No job runs found in the last {lookback_days} days.")
    st.stop()

df = pd.DataFrame(records)
df["run_time"] = df["run_time"].apply(lambda x: x.replace(tzinfo=None))
df["date"] = df["run_time"].dt.normalize()
df_last = (
    df.sort_values("run_time")
    .groupby(["job", "date"], as_index=False)
    .last()
)

# Only jobs that have runs in the period
job_names = sorted(df_last["job"].unique())

# Full grid: all jobs × all days in lookback period
all_dates = pd.date_range(
    end=dt.datetime(now_local.year, now_local.month, now_local.day),
    periods=lookback_days,
    freq="D",
)
full_grid = pd.DataFrame(
    [(job, date) for job in job_names for date in all_dates],
    columns=["job", "date"],
)
df_last_dedup = df_last.drop_duplicates(["job", "date"])

df_grid = full_grid.merge(
    df_last_dedup[["job", "date", "status", "run_time", "duration_min"]],
    on=["job", "date"],
    how="left",
).drop_duplicates(["job", "date"])
df_grid["status"] = df_grid["status"].fillna("NO RUN")

status_colors = {
    "SUCCESS": "#66BB6A",
    "FAILED": "#EF5350",
    "CANCELED": "#707070",
    "RUNNING": "#EFC550",
    "NO RUN": "#EEEEEE",
}

# Worst status per job for label coloring (FAILED > CANCELED > RUNNING > SUCCESS > NO RUN)
_priority = {"FAILED": 0, "CANCELED": 1, "RUNNING": 2, "SUCCESS": 3, "NO RUN": 4}
df_worst = (
    df_grid.groupby("job")["status"]
    .agg(lambda s: min(s, key=lambda x: _priority.get(x, 9)))
    .reset_index()
    .rename(columns={"status": "worst_status"})
)

label_colors = {
    "FAILED":  "#EF5350",
    "CANCELED": "#707070",
    "RUNNING":  "#EFC550",
    "SUCCESS":  "#31333F",
    "NO RUN":   "#AAAAAA",
}

_ws_host = w.config.host.rstrip("/")
job_to_url = {name: f"{_ws_host}/jobs/{jid}" for name, jid in job_to_id.items()}
job_worst = dict(zip(df_worst["job"], df_worst["worst_status"]))

heatmap = (
    alt.Chart(df_grid)
    .mark_rect(stroke="white", strokeWidth=2)
    .encode(
        x=alt.X(
            "yearmonthdate(date):O",
            title="Date",
            axis=alt.Axis(labelAngle=-45, format="%m-%d"),
        ),
        y=alt.Y(
            "job:N",
            title="",
            sort=job_names,
            axis=alt.Axis(labels=False, ticks=False, domain=False),
        ),
        color=alt.Color(
            "status:N",
            scale=alt.Scale(
                domain=list(status_colors.keys()),
                range=list(status_colors.values()),
            ),
            legend=alt.Legend(title="Status"),
        ),
        tooltip=[
            "job",
            "status",
            alt.Tooltip("run_time:T", title="Last Run Time", format="%Y-%m-%d %H:%M"),
            alt.Tooltip("duration_min:Q", title="Duration (min)"),
        ],
    )
    .properties(height=alt.Step(25))
)

chart = heatmap

st.markdown("""
<style>
div[data-testid="stVerticalBlock"] {
    gap: 0 !important;
    min-height: 0 !important;
    overflow: visible !important;
}
div[data-testid="column"],
div[data-testid="stHorizontalBlock"] {
    overflow: visible !important;
}
div[data-testid="element-container"]:has(.stButton) {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 0 !important;
}
button[data-testid="stBaseButton-secondary"] {
    height: 25px !important;
    min-height: 25px !important;
    padding: 0 4px !important;
    margin: 0 !important;
    border: none !important;
    background: transparent !important;
    box-shadow: none !important;
    font-size: 10px !important;
    display: flex !important;
    flex-direction: column !important;
    justify-content: flex-end !important;
    align-items: center !important;
}
button[data-testid="stBaseButton-secondary"]:hover {
    background: rgba(49,51,63,0.08) !important;
    border: none !important;
}
div[data-testid="stButton"] {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1 !important;
    width: 100% !important;
}
div[data-testid="stButton"] > div,
div[data-testid="stButton"] > div > div,
div.stTooltipIcon,
div[data-testid="stTooltipHoverTarget"] {
    width: 100% !important;
    padding: 0 !important;
    margin: 0 !important;
}
div[data-testid="stTooltipHoverTarget"] {
    justify-content: center !important;
}
div[data-testid="stButton"] > button,
div[data-testid="stButton"] > div button {
    width: 100% !important;
    padding: 0 !important;
}
button[data-testid="stBaseButton-secondary"] p {
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1 !important;
}
[data-testid="stMarkdownContainer"] p {
    font-size: 0.6rem !important;
}
.job-labels {
    padding-top: 5px;
    display: flex;
    flex-direction: column;
}
.job-labels a {
    height: 25px;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    font-size: 11px;
    text-decoration: none;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    padding-right: 4px;
}
.job-labels a:hover {
    text-decoration: underline;
}
</style>
""", unsafe_allow_html=True)

col_btn, col_labels, col_chart = st.columns([0.02, 0.15, 0.83])
triggered_job = None

_label_html = '<div class="job-labels">' + "".join(
    f'<a href="{job_to_url.get(jname, "#")}" target="_blank" rel="noopener noreferrer" '
    f'style="color:{label_colors[job_worst.get(jname, "NO RUN")]};\" title="{jname}">{jname}</a>'
    for jname in job_names
) + "</div>"

with col_labels:
    st.markdown(_label_html, unsafe_allow_html=True)

with col_chart:
    st.altair_chart(chart, use_container_width=True)

with col_btn:
    for jname in job_names:
        jid = job_to_id.get(jname)
        running_run_id = job_to_running_run_id.get(jname)
        if running_run_id:
            if st.button("■", key=f"stop_{jname}_{running_run_id}", use_container_width=True):
                triggered_job = ("stop", jname, int(running_run_id))
        elif jid:
            if st.button("▶", key=f"run_{jname}_{jid}", use_container_width=True):
                triggered_job = ("run", jname, int(jid))

if triggered_job:
    action, jname, id_ = triggered_job
    if action == "run":
        try:
            run_result = user_w.jobs.run_now(job_id=id_)
            st.success(f"Job **{jname}** started — run ID: {run_result.run_id}")
        except Exception as e:
            st.error(f"Failed to start **{jname}**: {e}")
    else:
        try:
            user_w.jobs.cancel_run(run_id=id_)
            st.success(f"Job **{jname}** stop requested — run ID: {id_}")
        except Exception as e:
            st.error(f"Failed to stop **{jname}**: {e}")
