"""
Customer Support Query Bot — Streamlit App
Deploy on Streamlit Community Cloud with Databricks backend.
"""

import streamlit as st
from databricks import sql as databricks_sql
import pandas as pd

# ──────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Customer Support Query Bot",
    page_icon="🔍",
    layout="centered",
)

# ──────────────────────────────────────────────────────────────────────
# Databricks connection (reads credentials from Streamlit secrets)
# ──────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    """
    Connect to Databricks SQL warehouse.
    Reads credentials from .streamlit/secrets.toml (local)
    or Streamlit Community Cloud secrets (deployed).
    """
    return databricks_sql.connect(
        server_hostname=st.secrets["databricks"]["server_hostname"],
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["access_token"],
    )


def run_query(sql: str) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)


# ──────────────────────────────────────────────────────────────────────
# SQL queries
# ──────────────────────────────────────────────────────────────────────

def query_login_by_email(email: str) -> pd.DataFrame:
    sql = f"""
        SELECT login_id, id, email
        FROM silver.basic.customers
        WHERE email ILIKE '%{email}%'
    """
    return run_query(sql)


def query_login_by_id(customer_id: str) -> pd.DataFrame:
    sql = f"""
        SELECT login_id, id, email
        FROM silver.basic.customers
        WHERE CAST(id AS TEXT) = '{customer_id}'
    """
    return run_query(sql)


def query_email_changes(customer_id: str) -> pd.DataFrame:
    sql = f"""
        SELECT
            item_id AS customer_id,
            event,
            FROM_UTC_TIMESTAMP(created_at, 'America/Chicago') AS date_time,
            CASE
                WHEN v.object_changes ILIKE '%current_sign_in_ip%' THEN 'current_sign_in_ip'
                WHEN v.object_changes ILIKE '%email%' THEN 'email'
                WHEN v.object_changes ILIKE '%reset_password_token%' THEN 'reset_password_token'
                WHEN v.object_changes ILIKE '%password%' THEN 'password'
                WHEN v.object_changes ILIKE '%failed_attempts%' THEN 'failed_attempts'
                WHEN v.object_changes ILIKE '%last_sign_in_at%' THEN 'last_sign_in_at'
                WHEN v.object_changes ILIKE '%login_id%' THEN 'login_id'
                WHEN v.object_changes ILIKE '%privacy_policy_consent%' THEN 'privacy_policy_consent'
                WHEN v.object_changes ILIKE '%session_token%' THEN 'session_token'
                ELSE 'other'
            END AS change_type,
            CASE
                WHEN v.object_changes ILIKE '%current_sign_in_ip%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'current_sign_in_ip:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%email%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'email:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%reset_password_token%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'reset_password_token:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%password%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'password:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%failed_attempts%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'failed_attempts:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%last_sign_in_at%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'last_sign_in_at:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%login_id%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'login_id:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%privacy_policy_consent%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'privacy_policy_consent:', 2), '- ', 2), '\\n', 1)
                WHEN v.object_changes ILIKE '%session_token%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'session_token:', 2), '- ', 2), '\\n', 1)
                ELSE NULL
            END AS old_value,
            CASE
                WHEN v.object_changes ILIKE '%current_sign_in_ip%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'current_sign_in_ip:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%email%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'email:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%reset_password_token%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'reset_password_token:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%password%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'password:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%failed_attempts%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'failed_attempts:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%last_sign_in_at%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'last_sign_in_at:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%login_id%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'login_id:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%privacy_policy_consent%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'privacy_policy_consent:', 2), '- ', 3), '\\n', 1)
                WHEN v.object_changes ILIKE '%session_token%' THEN SPLIT_PART(SPLIT_PART(SPLIT_PART(v.object_changes, 'session_token:', 2), '- ', 3), '\\n', 1)
                ELSE NULL
            END AS new_value,
            object_changes
        FROM silver.basic.versions v
        WHERE v.item_type = 'Customer'
            AND (object_changes ILIKE '%password%' OR object_changes ILIKE '%email%' OR object_changes ILIKE '%fail%')
            AND v.item_id = {customer_id}
        ORDER BY 3 DESC
    """
    return run_query(sql)


# ──────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────

st.title("Customer Support Query Bot")
st.markdown("---")

# Initialize session state for tracking the flow
if "step" not in st.session_state:
    st.session_state.step = "question_1"

if "results" not in st.session_state:
    st.session_state.results = None


def restart():
    """Reset everything back to the first question."""
    st.session_state.step = "question_1"
    st.session_state.results = None


# ──────────────────────────────────────────────────────────
# Question 1: Login issue?
# ──────────────────────────────────────────────────────────

st.subheader("Is the customer having a hard time logging in?")

col1, col2, col3 = st.columns([1, 1, 2])

with col1:
    if st.button("Yes", key="login_yes", use_container_width=True, type="primary"):
        st.session_state.step = "login_lookup"
        st.session_state.results = None
        st.rerun()

with col2:
    if st.button("No", key="login_no", use_container_width=True):
        st.session_state.step = "question_2"
        st.session_state.results = None
        st.rerun()

# ──────────────────────────────────────────────────────────
# Path 1: Login lookup
# ──────────────────────────────────────────────────────────

if st.session_state.step == "login_lookup":
    st.markdown("---")
    st.subheader("Look up customer login")

    query_type = st.radio(
        "Search by:",
        ["Email", "Customer ID"],
        horizontal=True,
    )

    search_value = st.text_input(
        "Enter email address:" if query_type == "Email" else "Enter customer ID:",
        placeholder="jane@example.com" if query_type == "Email" else "12345",
    )

    col_search, col_restart = st.columns([1, 1])

    with col_search:
        if st.button("Search", type="primary", use_container_width=True):
            if not search_value.strip():
                st.warning("Please enter a search value.")
            else:
                with st.spinner("Querying..."):
                    if query_type == "Email":
                        df = query_login_by_email(search_value.strip())
                    else:
                        df = query_login_by_id(search_value.strip())
                    st.session_state.results = df

    with col_restart:
        st.button("Start Over", on_click=restart, use_container_width=True)

    # Show results
    if st.session_state.results is not None:
        df = st.session_state.results
        if df.empty:
            st.info(f"No results found for: {search_value}")
        else:
            st.success(f"Found {len(df)} result(s)")
            st.dataframe(df, use_container_width=True, hide_index=True)

# ──────────────────────────────────────────────────────────
# Question 2: Email change?
# ──────────────────────────────────────────────────────────

if st.session_state.step == "question_2":
    st.markdown("---")
    st.subheader("Has there been a change to the customer email?")

    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        if st.button("Yes", key="email_yes", use_container_width=True, type="primary"):
            st.session_state.step = "email_change_lookup"
            st.session_state.results = None
            st.rerun()

    with col2:
        if st.button("No", key="email_no", use_container_width=True):
            st.session_state.step = "no_issues"
            st.rerun()

# ──────────────────────────────────────────────────────────
# Path 2: Email change lookup
# ──────────────────────────────────────────────────────────

if st.session_state.step == "email_change_lookup":
    st.markdown("---")
    st.subheader("Look up customer email changes")

    customer_id = st.text_input(
        "Enter customer ID:",
        placeholder="12345",
        key="email_change_id",
    )

    col_search, col_restart = st.columns([1, 1])

    with col_search:
        if st.button("Search", key="email_search", type="primary", use_container_width=True):
            if not customer_id.strip():
                st.warning("Please enter a customer ID.")
            else:
                with st.spinner("Querying account changes..."):
                    df = query_email_changes(customer_id.strip())
                    st.session_state.results = df

    with col_restart:
        st.button("Start Over", on_click=restart, use_container_width=True, key="restart_email")

    # Show results
    if st.session_state.results is not None:
        df = st.session_state.results
        if df.empty:
            st.info(f"No account changes found for customer ID: {customer_id}")
        else:
            st.success(f"Found {len(df)} change(s)")
            st.dataframe(df, use_container_width=True, hide_index=True)

# ──────────────────────────────────────────────────────────
# No issues
# ──────────────────────────────────────────────────────────

if st.session_state.step == "no_issues":
    st.markdown("---")
    st.info("No issues identified.")
    st.button("Start Over", on_click=restart, key="restart_none")
