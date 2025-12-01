"""
app_full.py
Streamlit app: Excel Data Structuring Tool (Integrated)
Features:
- Upload .xlsx (multi-sheet)
- Interactive form to add operations to a pipeline (Step 2)
- Parse and apply pipeline to selected sheets (Step 3)
- Preview before final output (first N rows; side-by-side)
- Generate downloadable .xlsx with transformed sheets (Step 4)
- Save / load pipeline templates (JSON)
- Logging to app.log
- All functions include docstrings and inline comments for clarity
"""

from io import BytesIO
import json
import os
import logging
from typing import Dict, List, Any, Tuple, Optional

import pandas as pd
import streamlit as st

# ---------------------------
# Setup logging
# ---------------------------
LOG_FILE = "app.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("excel-structuring-tool")

# ---------------------------
# Excel Content Auto-Detection (New Feature)
# ---------------------------

from dateutil.parser import parse

def detect_excel_category(df: pd.DataFrame) -> str:
    """
    Guess dataset category based on column names and data patterns.
    Returns: category string.
    """
    cols = [c.lower() for c in df.columns]

    keywords = {
        "Sales / Finance": ["amount", "price", "total", "revenue", "invoice", "qty", "product"],
        "HR / Employees": ["employee", "empid", "salary", "department", "designation"],
        "Students / Education": ["student", "roll", "marks", "grade", "course"],
        "Inventory / Stock": ["stock", "sku", "inventory", "warehouse", "quantity"],
    }

    # Keyword detection
    for category, words in keywords.items():
        for w in words:
            if any(w in c for c in cols):
                return category

    # Check date + numeric → time-series
    date_cols = 0
    for c in df.columns:
        try:
            parse(str(df[c].iloc[0]))
            date_cols += 1
        except:
            pass

    if date_cols >= 1 and len(df.select_dtypes(include="number").columns) >= 1:
        return "Time Series"

    return "Unknown"


def dataset_quick_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Quick stats summary.
    """
    summary = {
        "Total Rows": len(df),
        "Total Columns": len(df.columns),
        "Numeric Columns": len(df.select_dtypes(include='number').columns),
        "String Columns": len(df.select_dtypes(include='object').columns),
        "Missing Values": df.isna().sum().sum(),
    }
    return pd.DataFrame(summary.items(), columns=["Metric", "Value"])


# ---------------------------
# Utilities / Transform handlers
# ---------------------------
def safe_get_column_series(df: pd.DataFrame, col: str):
    """
    Return column series if exists, else raise KeyError.
    """
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found.")
    return df[col]

def op_remove_duplicates(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Remove duplicates from DataFrame.
    params:
      - subset: list[str] or None
      - keep: 'first'|'last'|False
    """
    subset = params.get("subset", None)
    keep = params.get("keep", "first")
    return df.drop_duplicates(subset=subset if subset else None, keep=keep)

def op_filter_rows(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Filter rows using a pandas query string.
    params:
      - condition: str (pandas query, e.g., "Age > 25 and Status == 'Active'")
    """
    condition = params.get("condition", "")
    if not condition:
        return df
    try:
        return df.query(condition)
    except Exception as e:
        raise ValueError(f"Invalid filter condition: {e}")

def op_replace_values(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Replace values in a column using mapping or scalar.
    params:
      - column: str
      - map: dict OR value pairs (old->new)
    """
    col = params["column"]
    mapping = params.get("map", None)
    old = params.get("old", None)
    new = params.get("new", None)

    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found for replace.")
    if mapping:
        return df.assign(**{col: df[col].replace(mapping)})
    else:
        return df.assign(**{col: df[col].replace({old: new})})

def op_merge_columns(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Merge multiple columns into a new column.
    params:
      - new_column: str
      - columns: list[str]
      - sep: str
    """
    new_col = params["new_column"]
    cols = params.get("columns", [])
    sep = params.get("sep", " ")
    for c in cols:
        if c not in df.columns:
            raise KeyError(f"Column '{c}' not found for merge.")
    return df.assign(**{new_col: df[cols].astype(str).agg(sep.join, axis=1)})

def op_convert_date(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert date column formats.
    params:
      - column: str (existing)
      - fmt_out: str (strftime format, e.g., '%Y-%m-%d')
    """
    col = params["column"]
    fmt_out = params.get("fmt_out", None)
    if col not in df.columns:
        raise KeyError(f"Date column '{col}' not found.")
    # parse with pandas (coerce invalid)
    parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
    if fmt_out:
        return df.assign(**{col: parsed.dt.strftime(fmt_out)})
    else:
        return df.assign(**{col: parsed})

def op_normalize_text(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize text in a column.
    params:
      - column: str
      - mode: 'lowercase'|'uppercase'|'title'|'strip'
    """
    col = params["column"]
    mode = params.get("mode", "lowercase")
    if col not in df.columns:
        raise KeyError(f"Text column '{col}' not found.")
    s = df[col].astype(str)
    if mode == "lowercase":
        s = s.str.lower()
    elif mode == "uppercase":
        s = s.str.upper()
    elif mode == "title":
        s = s.str.title()
    elif mode == "strip":
        s = s.str.strip()
    return df.assign(**{col: s})

def op_math(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Create new column via math expression or simple two-column op.
    params:
      - new_column: str
      - expr: str (pandas.eval style; limited)
      - mode: optional simple mode keys if used
    """
    new_col = params["new_column"]
    expr = params.get("expr", None)
    if expr:
        # Evaluate expression in dataframe context using pandas.eval (safer than eval)
        # We convert DF to local dict of Series for pd.eval
        try:
            # local_dict will allow column names to be used as variables
            local = df.to_dict("series")
            result = pd.eval(expr, engine="python", local_dict=local)
            df = df.copy()
            df[new_col] = result
            return df
        except Exception as e:
            raise ValueError(f"Invalid math expression '{expr}': {e}")
    else:
        raise ValueError("No expression provided for math operation.")

def op_aggregate(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Aggregate DataFrame by group-by columns.
    params:
      - by: list[str]
      - agg: dict (col -> aggfunc)
    """
    by = params.get("by", [])
    agg = params.get("agg", {})
    missing = [c for c in by if c not in df.columns]
    if missing:
        raise KeyError(f"Group-by columns not found: {missing}")
    try:
        return df.groupby(by).agg(agg).reset_index()
    except Exception as e:
        raise ValueError(f"Aggregation failed: {e}")

def op_conditional(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Create new column using a conditional expression.
    params:
      - new_column: str
      - condition: str (pandas.query style)
      - true_val: scalar
      - false_val: scalar
    """
    new_col = params["new_column"]
    condition = params["condition"]
    true_val = params.get("true_val")
    false_val = params.get("false_val")
    try:
        mask = df.eval(condition)
    except Exception as e:
        raise ValueError(f"Invalid conditional expression '{condition}': {e}")
    df = df.copy()
    df[new_col] = false_val
    df.loc[mask, new_col] = true_val
    return df

def op_sort_data(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Sort DataFrame by specified columns.
    params:
      - by: list[str] or str (column names)
      - ascending: bool or list[bool]
    """
    by = params.get("by", [])
    ascending = params.get("ascending", True)
    if isinstance(by, str):
        by = [by]
    missing = [c for c in by if c not in df.columns]
    if missing:
        raise KeyError(f"Sort columns not found: {missing}")
    return df.sort_values(by=by, ascending=ascending).reset_index(drop=True)

def op_validate_pattern(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Add validation column checking if values match a pattern.
    params:
      - column: str
      - pattern: str (regex pattern)
      - new_column: str (validation result column)
    """
    import re
    col = params["column"]
    pattern = params["pattern"]
    new_col = params["new_column"]
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found for validation.")
    df = df.copy()
    df[new_col] = df[col].astype(str).str.match(pattern, na=False)
    return df

def op_remove_blank_rows(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Remove rows that are completely blank or have blank values in specified columns.
    params:
      - columns: list[str] or None (if None, removes completely empty rows)
      - how: 'any' or 'all' (any blank column or all blank columns)
    """
    columns = params.get("columns", None)
    how = params.get("how", "any")
    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            raise KeyError(f"Columns not found: {missing}")
        return df.dropna(subset=columns, how=how)
    else:
        return df.dropna(how="all")

# Map op names to handler functions
OP_HANDLERS = {
    "remove_duplicates": op_remove_duplicates,
    "filter_rows": op_filter_rows,
    "replace_values": op_replace_values,
    "merge_columns": op_merge_columns,
    "convert_date": op_convert_date,
    "normalize_text": op_normalize_text,
    "math": op_math,
    "aggregate": op_aggregate,
    "conditional": op_conditional,
    "sort_data": op_sort_data,
    "validate_pattern": op_validate_pattern,
    "remove_blank_rows": op_remove_blank_rows,
}

def apply_pipeline_to_df(df: pd.DataFrame, pipeline: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[str]]:
    """
    Apply a list of transformation steps (pipeline) to a DataFrame.
    Returns transformed DataFrame and operation log (list of human-readable strings).
    """
    op_log: List[str] = []
    current = df.copy()
    for idx, step in enumerate(pipeline):
        op_name = step.get("op")
        params = {k: v for k, v in step.items() if k != "op"}
        op_log.append(f"Step {idx+1}: {op_name} {params}")
        logger.info("Applying step %d: %s %s", idx+1, op_name, params)
        if op_name not in OP_HANDLERS:
            msg = f"Unsupported operation: {op_name}"
            logger.error(msg)
            raise ValueError(msg)
        try:
            handler = OP_HANDLERS[op_name]
            current = handler(current, params)
        except Exception as e:
            # Log and re-raise so UI can present it
            logger.exception("Error applying operation '%s': %s", op_name, e)
            raise
    return current, op_log

# ---------------------------
# Template helpers
# ---------------------------
TEMPLATES_DIR = "templates"
os.makedirs(TEMPLATES_DIR, exist_ok=True)

def save_template(name: str, pipeline: List[Dict[str, Any]]) -> str:
    """
    Save pipeline JSON into templates dir. Returns path.
    """
    fname = os.path.join(TEMPLATES_DIR, f"{name}.json")
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(pipeline, f, indent=2)
    logger.info("Saved template: %s", fname)
    return fname

def load_template_from_path(path: str) -> List[Dict[str, Any]]:
    """
    Load pipeline JSON from a given path (local file path).
    """
    with open(path, "r", encoding="utf-8") as f:
        pipeline = json.load(f)
    return pipeline

def list_templates() -> List[str]:
    """
    List available template filenames (without extension).
    """
    files = [os.path.splitext(f)[0] for f in os.listdir(TEMPLATES_DIR) if f.endswith(".json")]
    return files

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="Excel Data Structuring Tool", layout="wide")
st.title("📦 Excel Data Structuring Tool — Full")

st.markdown(
    "Upload an Excel file, add transformations via the UI, preview changes, then download the transformed Excel file. "
)

# File upload (multi-sheet)
uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], accept_multiple_files=False)
if not uploaded:
    st.info("Upload an Excel file to start. Sample files can be placed in the repo.")
    st.stop()

# Read all sheets into dict
try:
    # For typical files we read all sheets. For very large files you may want streaming.
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(uploaded, sheet_name=None)
    sheet_names = list(sheets.keys())
    st.sidebar.success(f"Loaded {len(sheet_names)} sheet(s).")
except Exception as e:
    st.error(f"Failed to read Excel file: {e}")
    logger.exception("Failed to read uploaded Excel.")
    st.stop()
    
# ---------------------------
# Auto-detection Display Panel
# ---------------------------
st.subheader("🔍 Auto-Detected Dataset Insights")

first_sheet = sheet_names[0]
df_first = sheets[first_sheet]

category = detect_excel_category(df_first)
summary_df = dataset_quick_summary(df_first)

st.info(f"**Detected Category:** {category}")
st.markdown("### Quick Summary")
st.table(summary_df)

# Optional: auto-chart for numeric columns
numeric_cols = df_first.select_dtypes(include="number").columns.tolist()
if numeric_cols:
    st.markdown("### Auto-Generated Preview Chart")
    st.line_chart(df_first[numeric_cols[0]])
    st.caption(f"Chart based on `{numeric_cols[0]}` column.")
else:
    st.caption("No numeric columns available for charting.")


# Select which sheet to preview / operate on
st.sidebar.header("Select sheet(s) to operate on")
apply_to_all = st.sidebar.checkbox("Apply pipeline to ALL sheets", value=False)
if not apply_to_all:
    selected_sheet = st.sidebar.selectbox("Choose sheet for preview / operation", sheet_names)
else:
    selected_sheet = sheet_names[0]  # preview one if all

# Show preview of selected sheet
df_original = sheets[selected_sheet]
st.subheader(f"Preview — Original (sheet: {selected_sheet})")
st.dataframe(df_original.head(10))

# Pipeline stored in session state for interactive building
if "pipeline" not in st.session_state:
    st.session_state.pipeline = []

st.subheader("Step 2 — Add operations (form-based)")

# Operation selector
op_choice = st.selectbox(
    "Choose operation to add",
    [
        "Select...",
        "Remove duplicates",
        "Remove blank rows",
        "Filter rows",
        "Sort data",
        "Replace values",
        "Merge columns",
        "Convert date formats",
        "Normalize text",
        "Math operation",
        "Aggregate",
        "Conditional calculation",
        "Validate patterns"
    ],
    index=0
)

# Dynamic UI for each operation
def add_operation_to_pipeline(step: Dict[str, Any]):
    """
    Insert operation step dict to session pipeline and log action.
    """
    st.session_state.pipeline.append(step)
    st.success(f"Added: {step.get('op')}")
    logger.info("Pipeline appended: %s", step)

# Remove duplicates UI
if op_choice == "Remove duplicates":
    subset_cols = st.multiselect("Columns to consider for duplicates (leave empty for all columns)", df_original.columns.tolist())
    keep = st.selectbox("Keep which duplicate?", ["first", "last", False])
    if st.button("Add Remove Duplicates"):
        step = {"op": "remove_duplicates", "subset": subset_cols if subset_cols else None, "keep": keep}
        add_operation_to_pipeline(step)

# Filter rows UI
elif op_choice == "Filter rows":
    condition = st.text_input("Condition (pandas query syntax). Example: Age > 25 and Status == 'Active'")
    if st.button("Add Filter"):
        add_operation_to_pipeline({"op": "filter_rows", "condition": condition})

# Replace values UI
elif op_choice == "Replace values":
    col = st.selectbox("Select column", df_original.columns)
    old_val = st.text_input("Old value (exact match)")
    new_val = st.text_input("New value")
    if st.button("Add Replace"):
        add_operation_to_pipeline({"op": "replace_values", "column": col, "old": old_val, "new": new_val})

# Merge columns UI
elif op_choice == "Merge columns":
    cols = st.multiselect("Columns to merge", df_original.columns)
    sep = st.text_input("Separator", " ")
    new_col = st.text_input("New column name")
    if st.button("Add Merge"):
        add_operation_to_pipeline({"op": "merge_columns", "columns": cols, "sep": sep, "new_column": new_col})

# Convert date UI
elif op_choice == "Convert date formats":
    date_col = st.selectbox("Date column", df_original.columns)
    fmt_out = st.text_input("Output format (strftime). Example: %Y-%m-%d")
    if st.button("Add Date Convert"):
        add_operation_to_pipeline({"op": "convert_date", "column": date_col, "fmt_out": fmt_out})

# Normalize text UI
elif op_choice == "Normalize text":
    txt_col = st.selectbox("Text column", df_original.columns)
    mode = st.selectbox("Mode", ["lowercase", "uppercase", "title", "strip"])
    if st.button("Add Normalize"):
        add_operation_to_pipeline({"op": "normalize_text", "column": txt_col, "mode": mode})

# Math op UI
elif op_choice == "Math operation":
    new_col = st.text_input("New column name (result)")
    expr = st.text_input("Expression (use column names as variables). Example: (Revenue - Cost) / Cost * 100")
    st.markdown("**Note:** Expressions are evaluated with pandas.eval in a limited context.")
    if st.button("Add Math"):
        add_operation_to_pipeline({"op": "math", "new_column": new_col, "expr": expr})

# Aggregate UI
elif op_choice == "Aggregate":
    group_by = st.multiselect("Group by columns", df_original.columns)
    agg_col = st.selectbox("Column to aggregate", df_original.columns)
    agg_func = st.selectbox("Aggregation", ["sum", "mean", "median", "min", "max"])
    if st.button("Add Aggregate"):
        add_operation_to_pipeline({"op": "aggregate", "by": group_by, "agg": {agg_col: agg_func}})

# Conditional UI
elif op_choice == "Conditional calculation":
    cond = st.text_input("Condition (pandas eval syntax). Example: Age > 30")
    true_v = st.text_input("Value if True")
    false_v = st.text_input("Value if False")
    new_col = st.text_input("New column name")
    if st.button("Add Conditional"):
        add_operation_to_pipeline({"op": "conditional", "condition": cond, "true_val": true_v, "false_val": false_v, "new_column": new_col})

# Sort data UI
elif op_choice == "Sort data":
    sort_cols = st.multiselect("Columns to sort by", df_original.columns)
    ascending = st.checkbox("Ascending order", value=True)
    if st.button("Add Sort"):
        add_operation_to_pipeline({"op": "sort_data", "by": sort_cols, "ascending": ascending})

# Remove blank rows UI
elif op_choice == "Remove blank rows":
    blank_cols = st.multiselect("Check these columns for blanks (leave empty to check all)", df_original.columns)
    how = st.selectbox("Remove if", ["any", "all"], help="any: remove if ANY selected column is blank, all: remove if ALL selected columns are blank")
    if st.button("Add Remove Blanks"):
        add_operation_to_pipeline({"op": "remove_blank_rows", "columns": blank_cols if blank_cols else None, "how": how})

# Validate patterns UI
elif op_choice == "Validate patterns":
    val_col = st.selectbox("Column to validate", df_original.columns)
    pattern = st.text_input("Regex pattern (e.g., '^\\d{10}$' for 10-digit numbers)")
    result_col = st.text_input("Result column name", "is_valid")
    st.markdown("**Examples:** Account numbers: `^\\d{10}$`, Email: `^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$`")
    if st.button("Add Validation"):
        add_operation_to_pipeline({"op": "validate_pattern", "column": val_col, "pattern": pattern, "new_column": result_col})

# Show current pipeline
st.subheader("Current Pipeline")
if st.session_state.pipeline:
    for i, step in enumerate(st.session_state.pipeline, 1):
        st.markdown(f"**{i}.** `{step.get('op')}` — `{ {k:v for k,v in step.items() if k != 'op'} }`")
    if st.button("Clear pipeline"):
        st.session_state.pipeline = []
        st.success("Pipeline cleared.")
else:
    st.info("No operations in pipeline. Add operations from the dropdown above.")

# Template save / load
st.sidebar.header("Templates")
template_name = st.sidebar.text_input("Template name (to save current pipeline)")
if st.sidebar.button("Save template"):
    if not st.session_state.pipeline:
        st.sidebar.error("Pipeline empty — nothing to save.")
    elif not template_name:
        st.sidebar.error("Provide a name for template.")
    else:
        path = save_template(template_name, st.session_state.pipeline)
        st.sidebar.success(f"Saved template: {os.path.basename(path)}")

templates = list_templates()
selected_template = st.sidebar.selectbox("Load saved template", ["(none)"] + templates)
if st.sidebar.button("Load template"):
    if selected_template == "(none)":
        st.sidebar.error("Select a template to load.")
    else:
        path = os.path.join(TEMPLATES_DIR, f"{selected_template}.json")
        try:
            pipeline = load_template_from_path(path)
            st.session_state.pipeline = pipeline
            st.sidebar.success(f"Loaded template '{selected_template}'.")
        except Exception as e:
            st.sidebar.error(f"Failed to load template: {e}")
            logger.exception("Failed to load template: %s", e)

# Optional: upload template file
uploaded_template = st.sidebar.file_uploader("Upload pipeline JSON (to load)", type=["json"])
if uploaded_template:
    try:
        tpl = json.load(uploaded_template)
        st.session_state.pipeline = tpl
        st.sidebar.success("Uploaded pipeline loaded.")
    except Exception as e:
        st.sidebar.error(f"Invalid JSON template: {e}")

# Preview / Apply section
st.subheader("Step 3 — Preview & Apply")

preview_cols = st.number_input("Preview rows (per sheet)", min_value=3, max_value=200, value=10, step=1)

col1, col2 = st.columns(2)
with col1:
    if st.button("Preview apply to selected sheet"):
        try:
            pipeline = st.session_state.pipeline
            if not pipeline:
                st.warning("Pipeline is empty — nothing to preview.")
            else:
                df_preview, logs = apply_pipeline_to_df(df_original, pipeline)
                st.markdown("**Original (top rows)**")
                st.dataframe(df_original.head(preview_cols))
                st.markdown("**Transformed (top rows)**")
                st.dataframe(df_preview.head(preview_cols))
                st.markdown("**Operation log**")
                for l in logs:
                    st.write(l)
        except Exception as e:
            st.error(f"Preview failed: {e}")

with col2:
    if st.button("Preview apply to ALL sheets"):
        try:
            pipeline = st.session_state.pipeline
            if not pipeline:
                st.warning("Pipeline is empty — nothing to preview.")
            else:
                previews = {}
                for name, df in sheets.items():
                    try:
                        previews[name], _ = apply_pipeline_to_df(df, pipeline)
                    except Exception as e:
                        previews[name] = None
                        logger.exception("Preview error for sheet %s: %s", name, e)
                # Display previews summary
                for name, p_df in previews.items():
                    st.markdown(f"**Sheet: {name}**")
                    if p_df is None:
                        st.error("Preview failed for this sheet (check logs).")
                    else:
                        st.dataframe(p_df.head(preview_cols))
        except Exception as e:
            st.error(f"Preview-all failed: {e}")

# Final apply & download
st.subheader("Step 4 — Execute & Download")

email_option = st.checkbox("(Optional) Email output to (not implemented) — placeholder")

if st.button("Apply pipeline and Download transformed .xlsx"):
    try:
        pipeline = st.session_state.pipeline
        if not pipeline:
            st.warning("Pipeline empty — nothing to apply.")
        else:
            # Decide which sheets to transform
            target_sheets = sheet_names if apply_to_all else [selected_sheet]
            transformed_sheets: Dict[str, pd.DataFrame] = {}
            errors: Dict[str, str] = {}

            # Apply pipeline to each selected sheet
            for name in target_sheets:
                df = sheets[name]
                try:
                    transformed_df, logs = apply_pipeline_to_df(df, pipeline)
                    transformed_sheets[name] = transformed_df
                    logger.info("Transformed sheet '%s' successfully.", name)
                except Exception as e:
                    errors[name] = str(e)
                    logger.exception("Error transforming sheet '%s': %s", name, e)

            if errors:
                st.error("Some sheets failed to transform. See details below and app.log for full trace.")
                for s, msg in errors.items():
                    st.write(f"Sheet `{s}` error: {msg}")

            if transformed_sheets:
                # Build in-memory excel workbook with all transformed sheets
                out = BytesIO()
                with pd.ExcelWriter(out, engine="openpyxl") as writer:
                    for sheet_name, df_out in transformed_sheets.items():
                        df_out.to_excel(writer, sheet_name=sheet_name, index=False)
                out.seek(0)
                st.success("Transformation complete — download below.")
                st.download_button(
                    "⬇️ Download transformed workbook",
                    data=out.getvalue(),
                    file_name="transformed_workbook.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
    except Exception as e:
        st.error(f"Execution failed: {e}")
        logger.exception("Execution failed: %s", e)

st.markdown("---")
st.markdown("**Logs** (last lines from app.log)")
try:
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()[-20:]
        st.text("".join(lines))
except Exception:
    st.text("No log file yet.")
