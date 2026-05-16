import os
import asyncio
import streamlit as st
import pandas as pd
import requests
from dotenv import load_dotenv

# Import the core engine logic from your app.py file
from app import process_arbitrage_opportunities, BUTTERBASE_TABLE_URL, butterbase_headers

load_dotenv()

st.set_page_config(
    page_title="Arbitrage Swarm Control Plane",
    page_icon="🤖",
    layout="wide"
)

st.title("🤖 Autonomous E-Commerce Arbitrage & Sourcing Network")
st.markdown("### Powered by Z.ai (`glm-5.1`) & TokenRouter Model Gateway")

st.divider()

# --- SIDEBAR CONTROL PANEL ---
st.sidebar.header("🎛️ Control Panel")
st.sidebar.write("Manually dispatch data collection payloads and force multi-agent execution frames.")

MIN_NET_PROFIT = st.sidebar.slider("Minimum Margin Filter ($)", 0.0, 50.0, 15.0, step=5.0)
os.environ["MIN_NET_PROFIT"] = str(MIN_NET_PROFIT)

# Helper function to query what is currently inside your database
def fetch_butterbase_inventory():
    try:
        # Fetching directly via your app's designated schema table configuration
        res = requests.get(BUTTERBASE_TABLE_URL, headers=butterbase_headers, timeout=10)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        st.sidebar.error(f"Database sync connection lag: {e}")
    return []


def coerce_numeric(series):
    """Convert a pandas series to numeric values, preserving empty values as 0.0."""
    return pd.to_numeric(series, errors="coerce").fillna(0.0)

# --- PIPELINE TRICGER INTERFACE ---
if st.sidebar.button("⚡ Trigger Swarm Execution Framework", use_container_width=True):
    st.subheader("📡 Real-time Pipeline Execution Logs")
    log_container = st.empty()
    
    with st.spinner("Executing Bright Data collection loop & running agent evaluations..."):
        # Run your app's main async orchestrator context block
        pipeline_future = process_arbitrage_opportunities(batch_input={})
        results = asyncio.run(pipeline_future)
        
    if results.get("status") == "success":
        st.success(f"Pipeline finished! Ingested {results['items_ingested']} items. Saved {results['profitable_deals_saved']} high-margin matches.")
        if results["summary"]:
            st.json(results["summary"])
    else:
        st.error("Pipeline pipeline interface processing failure detected.")

st.divider()

# --- LIVE REPOSITORY METRICS MAPPING ---
st.subheader("🔥 Current Relational Inventory Dashboard")

live_db_data = fetch_butterbase_inventory()

if live_db_data:
    # Convert data layer elements directly into a clean DataFrame
    df = pd.DataFrame(live_db_data)
    
    # Clean up table metrics displays
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    if "potential_margin" in df.columns:
        df["potential_margin"] = coerce_numeric(df["potential_margin"])
    if "wholesale_price" in df.columns:
        df["wholesale_price"] = coerce_numeric(df["wholesale_price"])
    if "retail_price" in df.columns:
        df["retail_price"] = coerce_numeric(df["retail_price"])
        
    # Build upper metric cards for the judges
    m1, m2, m3 = st.columns(3)
    m1.metric("Database Rows Tracked", len(df))
    m2.metric("Highest Available Margin", f"${df['potential_margin'].max():.2f}" if not df.empty else "$0.00")
    m3.metric("Average Sourced Unit Cost", f"${df['wholesale_price'].mean():.2f}" if not df.empty else "$0.00")
    
    st.write("")
    
    # Interactive formatting rules to instantly point out high-converting rows
    def highlight_margins(val):
        try:
            val = float(val)
        except (TypeError, ValueError):
            val = 0.0
        if val >= 30:
            return 'background-color: #27ae60; color: white; font-weight: bold;'
        elif val >= 15:
            return 'background-color: #2ecc71; color: white;'
        return 'background-color: #f1c40f; color: black;'

    if "potential_margin" in df.columns:
        st.dataframe(df.style.map(highlight_margins, subset=["potential_margin"]), use_container_width=True)
else:
    st.info("📡 The Butterbase cloud database table is currently warming up or empty. Click the button in the left sidebar to execute your scrapper pipeline and register your first live product targets!")