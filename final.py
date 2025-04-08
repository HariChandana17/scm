import streamlit as st
import streamlit as st
from google.cloud import bigquery
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
import pydeck as pdk
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import numpy as np
import sys
import requests
import polyline
import traceback
import base64
import openpyxl
import db_dtypes
import asyncio
import logging
import re
import json
import urllib.request
import urllib.parse
import random
import datetime
from typing import List, Dict, Any, Optional, Union
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from google.auth import default, exceptions as google_auth_exceptions
from google.api_core.exceptions import GoogleAPIError, NotFound, Forbidden
from agno.models.google import Gemini as AgnoGemini
from agno.agent import Agent
from agno.team import Team
from agno.exceptions import ModelProviderError
from vertexai.generative_models import Content, FunctionDeclaration, GenerativeModel, Part, Tool
import vertexai.preview.generative_models as generative_models
from google.cloud import aiplatform
import sys
import subprocess

# --- Configuration ---
PROJECT_ID = "gebu-data-ml-day0-01-333910"
BQ_DATASET_ID = "supply_chain"
BQ_FORECAST_DATASET_ID = "demand_forecast"
LOCATION = "us-central1"
GOOGLE_MAPS_API_KEY = "AIzaSyDJjAkgtwN0weYaoKFud_Xn3h5YDNG1q14"
MODEL_ID = "gemini-2.0-flash"
ROUTE_AGENT_MODEL_ID = "gemini-2.0-flash"
NL_BQ_AGENT_MODEL_ID = "gemini-2.0-flash"
REPLENISH_AGENT_MODEL_ID = "gemini-2.0-flash"
TEAM_ROUTER_MODEL_ID = "gemini-2.0-flash"
BQ_LOCATIONS_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET_ID}.locations"
BQ_ROUTES_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET_ID}.routes"
BQ_FORECAST_TABLE_ID = f"{PROJECT_ID}.{BQ_FORECAST_DATASET_ID}.forecast1"
BQ_PRODUCTS_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET_ID}.product_inventory"
REPLENISH_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET_ID}.product_inventory"

cmd = f"gsutil cp gs://sales_forecast_hackathon/Order_Management.xlsx Order_management.xlsx"
subprocess.run(cmd, shell=True)
 
cmd = f"gsutil cp gs://sales_forecast_hackathon/Historical_Product_Demand.csv Historical_Product_Demand.csv"
subprocess.run(cmd, shell=True)
 
# File paths
ORDER_EXCEL_PATH = r"Order_Management.xlsx"
HISTORY_CSV_PATH = r"Historical_Product_Demand.csv"



# PyDeck Icon Configuration
DC_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png"
STORE_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png"
PIN_WIDTH = 25
PIN_HEIGHT = 41
PIN_ANCHOR_Y_FACTOR = 1.0
DC_LOC_ID = 'LOC0'

# Hardcoded Addresses for Chatbot
BASE_ADDRESSES = [
    '3610+Hacks+Cross+Rd+Memphis+TN', '1921+Elvis+Presley+Blvd+Memphis+TN', '149+Union+Avenue+Memphis+TN',
    '1034+Audubon+Drive+Memphis+TN', '1532+Madison+Ave+Memphis+TN', '706+Union+Ave+Memphis+TN',
    '3641+Central+Ave+Memphis+TN', '926+E+McLemore+Ave+Memphis+TN', '4339+Park+Ave+Memphis+TN',
    '600+Goodwyn+St+Memphis+TN', '2000+North+Pkwy+Memphis+TN', '262+Danny+Thomas+Pl+Memphis+TN',
    '125+N+Front+St+Memphis+TN', '5959+Park+Ave+Memphis+TN', '814+Scott+St+Memphis+TN',
    '1005+Tillman+St+Memphis+TN'
]

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Styling from app.py ---
APP_STYLE = """
<style>
    body {
        font-family: 'Inter', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        background-color: #f0f4f8;
        color: #333;
    }
    .custom-header-container {
        background-color: #ffffff;
        padding: 1.5rem 2rem;
        border-radius: 10px;
        border-left: 6px solid #1a73e8;
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08);
    }
    .custom-header-container h1 {
        color: #0d47a1;
        margin-bottom: 0.5rem;
        font-size: 2.1em;
        font-weight: 700;
        display: flex;
        align-items: center;
    }
    .custom-header-container h1 img {
        margin-right: 15px;
        height: 45px;
        vertical-align: middle;
    }
    .custom-header-container p {
        color: #455a64;
        font-size: 1.05rem;
        margin-bottom: 0;
        line-height: 1.5;
    }
    div[data-testid="stTabs"] {
        border-bottom: 2px solid #cbd5e1;
        margin-bottom: 0;
    }
    div[data-testid="stTabs"] button[data-baseweb="tab"] {
        padding: 14px 28px;
        margin: 0 5px -2px 0;
        font-size: 1.1em;
        font-weight: 500;
        min-height: 50px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        color: #475569;
        background-color: #f1f5f9;
        border: 1px solid #e2e8f0;
        border-bottom: none;
        border-radius: 8px 8px 0 0;
        transition: background-color 0.2s ease, color 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease;
        position: relative;
        top: 2px;
        cursor: pointer;
    }
    div[data-testid="stTabs"] button[data-baseweb="tab"]:hover {
        background-color: #e2e8f0;
        color: #1e3a8a;
        border-color: #cbd5e1;
    }
    div[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
        background-color: #ffffff;
        color: #0061ff;
        font-weight: 600;
        border-color: #cbd5e1;
        border-bottom: 2px solid #ffffff;
        top: 0px;
        box-shadow: 0 -2px 4px rgba(0, 0, 0, 0.04);
    }
    div[data-testid="stTabPanel"] {
        border: 1px solid #cbd5e1;
        border-top: none;
        padding: 30px 25px;
        border-radius: 0 0 10px 10px;
        margin-top: -2px;
        background-color: #ffffff;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.04);
        margin-bottom: 30px;
    }
    .tab-header {
        color: #004aad;
        font-weight: 700;
        border-bottom: 4px solid #0061ff;
        padding-bottom: 12px;
        background-color: #f8f9a;
        margin-top: 0px;
        margin-bottom: 35px;
        font-size: 1.9em;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .sub-header {
        color: #1e293b;
        font-weight: 600;
        margin-top: 30px;
        margin-bottom: 20px;
        font-size: 1.5em;
        padding-left: 15px;
        background-color: #f8f9fa;
        padding-top: 8px;
        padding-bottom: 8px;
        border-radius: 4px;
    }
    .card-container {
        display: flex;
        gap: 25px;
        margin-bottom: 30px;
        justify-content: space-around;
        flex-wrap: wrap;
    }
    .info-card, .warning-card, .success-card, .neutral-card {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 25px;
        box-shadow: 0 5px 10px rgba(0, 0, 0, 0.05);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        flex: 1;
        min-width: 180px;
    }
    .info-card:hover, .warning-card:hover, .success-card:hover, .neutral-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.08);
    }
    .card-label {
        font-size: 1em;
        color: #475569;
        margin-bottom: 10px;
        font-weight: 500;
    }
    .card-value {
        font-size: 2.2em;
        font-weight: 700;
    }
    .info-card { border-left: 6px solid #3b82f6; }
    .info-card .card-value { color: #3b82f6; }
    .warning-card { border-left: 6px solid #f59e0b; }
    .warning-card .card-value { color: #f59e0b; }
    .success-card { border-left: 6px solid #10b981; }
    .success-card .card-value { color: #10b981; }
    .neutral-card { border-left: 6px solid #64748b; }
    .neutral-card .card-value { color: #64748b; }
    .legend-container {
        margin-top: 10px;
        margin-bottom: 25px;
        padding: 15px 20px;
        background-color: #eef2f9;
        border: 1px solid #dbeafe;
        border-radius: 8px;
        display: flex;
        flex-wrap: wrap;
        gap: 20px;
        align-items: center;
    }
    .legend-title {
        font-weight: 600;
        margin-right: 15px;
        color: #1e3a8a;
    }
    .legend-item {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        border-radius: 16px;
        font-size: 0.95em;
        border: 1px solid transparent;
    }
    .legend-color-box {
        width: 14px;
        height: 14px;
        margin-right: 10px;
        border-radius: 4px;
        display: inline-block;
    }
    .legend-red { background-color: #fee2e2; border-color: #fecaca; color: #b91c1c;}
    .legend-red .legend-color-box { background-color: #ef4444; }
    .legend-orange { background-color: #ffedd5; border-color: #fed7aa; color: #c2410c;}
    .legend-orange .legend-color-box { background-color: #f97316; }
    .legend-green { background-color: #dcfce7; border-color: #bbf7d0; color: #15803d;}
    .legend-green .legend-color-box { background-color: #22c55e; }
    .section-divider {
        margin-top: 40px;
        margin-bottom: 40px;
        border-top: 1px solid #dde4ed;
    }
    .stDataFrame {
        margin-top: 15px;
        margin-bottom: 30px;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06);
        border: 1px solid #d1d9e6;
    }
    .stDataFrame thead th {
        background-color: #f1f5f9;
        color: #0f172a;
        font-weight: 600;
        border-bottom: 2px solid #cbd5e1;
        padding: 12px 15px;
        text-align: left;
        text-transform: uppercase;
        font-size: 0.85em;
        letter-spacing: 0.5px;
    }
    .stDataFrame tbody td {
        padding: 10px 15px;
        border-bottom: 1px solid #e2e8f0;
        vertical-align: middle;
    }
    .stDataFrame tbody tr:nth-child(odd) td {
        background-color: #f8fafc;
    }
    .stDataFrame tbody tr:hover td {
        background-color: #eef2f9;
    }
    .stPyDeckChart {
        margin-top: 25px;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.04);
        border: 1px solid #e2e8f0;
        margin-bottom: 25px;
    }
    .footer-caption {
        text-align: center;
        font-style: italic;
        color: #94a3b8;
        margin-top: 50px;
        border-top: 1px solid #e2e8f0;
        padding-top: 20px;
        font-size: 0.85em;
    }
    .deck-tooltip {
        background-color: rgba(0,0,0,0.75) !important;
        color: white !important;
        border-radius: 5px !important;
        padding: 8px 12px !important;
        font-size: 0.9em !important;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
    }
</style>
"""

# --- Helper Functions from app.py ---
@st.cache_resource
def get_bq_client():
    client = None
    credentials = None
    auth_method = "None"
    try:
        if 'gcp_service_account' in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
            client.query("SELECT 1").result()
            auth_method = "Streamlit Secrets"
            logger.info(f"BigQuery Connection Successful ({auth_method}).")
            return client
    except Exception as e:
        logger.error(f"Connection via Streamlit Secrets failed: {e}")
    try:
        if not client:
            client = bigquery.Client(project=PROJECT_ID)
            client.query("SELECT 1").result()
            auth_method = "Application Default Credentials (ADC)"
            logger.info(f"BigQuery Connection Successful ({auth_method}).")
            return client
    except Exception as e:
        logger.error(f"Connection via ADC failed: {e}")
    try:
        if not client:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
                client.query("SELECT 1").result()
                auth_method = "GOOGLE_APPLICATION_CREDENTIALS Env Var"
                logger.info(f"BigQuery Connection Successful ({auth_method}).")
                return client
            else:
                logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
    except Exception as e:
        logger.error(f"Connection via GOOGLE_APPLICATION_CREDENTIALS failed: {e}")
    logger.error("Fatal: Could not connect to BigQuery using any available method.")
    st.error("Could not connect to Google BigQuery. Please check credentials.", icon="🚨")
    return None

def load_excel(file_path, data_label="Data"):
    if not os.path.exists(file_path):
        st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
        return None
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = df.columns.str.strip()
        if df.empty: st.warning(f"{data_label} Warning: File is empty: `{os.path.basename(file_path)}`", icon="⚠️")
        return df
    except FileNotFoundError:
        st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
        return None
    except Exception as e:
        st.error(f"An error occurred while reading {data_label} file ({os.path.basename(file_path)}): {e}", icon="❌")
        return None

def load_csv(file_path, data_label="Data"):
    if not os.path.exists(file_path):
        st.error(f"{data_label} Error: CSV file not found at `{file_path}`", icon="❌")
        return None
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        if df.empty: st.warning(f"{data_label} Warning: CSV file is empty: `{os.path.basename(file_path)}`", icon="⚠️")
        return df
    except FileNotFoundError:
        st.error(f"{data_label} Error: CSV file not found at `{file_path}`", icon="❌")
        return None
    except pd.errors.EmptyDataError:
        st.warning(f"{data_label} Warning: CSV file is empty: `{os.path.basename(file_path)}`", icon="⚠️")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred while reading {data_label} CSV file ({os.path.basename(file_path)}): {e}", icon="❌")
        return None

@st.cache_data
def load_historical_demand_data():
    return load_csv(HISTORY_CSV_PATH, "Historical Demand")

@st.cache_data(ttl=1800)
def load_bigquery_inventory(_client):
    if not _client:
        st.error("BigQuery client not available. Cannot load inventory data.", icon="☁️")
        return None
    query = f"SELECT * FROM `{BQ_PRODUCTS_TABLE_ID}`"
    try:
        df = _client.query(query).to_dataframe(
            create_bqstorage_client=True,
            dtypes={"Price__USD_": pd.Float64Dtype(), "Quantity": pd.Int64Dtype(), "Discount____": pd.Float64Dtype(), "Demand__Required_": pd.Int64Dtype()}
        )
        column_mapping = {
            'Product_ID': 'Product ID', 'Product_Name': 'Product Name', 'Price__USD_': 'Price (USD)', 'Description': 'Description',
            'Quantity': 'Quantity', 'Discount____': 'Discount (%)', 'Country_of_Origin': 'Country of Origin', 'Demand__Required_': 'Demand (Required)'
        }
        rename_map = {bq_col: app_col for bq_col, app_col in column_mapping.items() if bq_col in df.columns}
        df = df[list(rename_map.keys())].rename(columns=rename_map)
        return df
    except Exception as e:
        st.error(f"Error loading inventory data from BigQuery table `{BQ_PRODUCTS_TABLE_ID}`: {e}", icon="☁️")
        print(traceback.format_exc())
        return None

@st.cache_data(ttl=600)
def get_available_weeks_riders(_client):
    if not _client: return pd.DataFrame({'WeekNo': [], 'RiderID': []})
    query = f"SELECT DISTINCT WeekNo, RiderID FROM `{BQ_ROUTES_TABLE_ID}` ORDER BY WeekNo DESC, RiderID ASC"
    try:
        df = _client.query(query).to_dataframe(create_bqstorage_client=True, dtypes={"WeekNo": pd.Int64Dtype()})
        return df
    except Exception as e:
        st.error(f"Error fetching week/rider data from BigQuery: {e}", icon="☁️")
        print(traceback.format_exc())
        return pd.DataFrame({'WeekNo': pd.Series(dtype='Int64'), 'RiderID': pd.Series(dtype='str')})

@st.cache_data(ttl=600)
def get_route_data(_client, week: int, rider: str):
    if not _client: return pd.DataFrame({'Seq': [], 'LocID': []})
    try:
        week_int = int(week)
    except (ValueError, TypeError):
        st.error(f"Invalid week number provided: {week}", icon="❌")
        return pd.DataFrame({'Seq': [], 'LocID': []})
    query = f"""
        SELECT Seq, LocID
        FROM `{BQ_ROUTES_TABLE_ID}`
        WHERE WeekNo = @week_no AND RiderID = @rider_id
        ORDER BY Seq ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("week_no", "INT64", week_int),
        bigquery.ScalarQueryParameter("rider_id", "STRING", rider)
    ])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        if 'Seq' in df.columns:
            df['Seq'] = pd.to_numeric(df['Seq'], errors='coerce').astype(pd.Int64Dtype())
            df.dropna(subset=['Seq'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Error fetching route data for W{week}, R{rider}: {e}", icon="☁️")
        print(traceback.format_exc())
        return pd.DataFrame({'Seq': [], 'LocID': []})

@st.cache_data(ttl=3600)
def get_location_data(_client, loc_ids: list):
    if not _client: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})
    if not loc_ids: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})
    valid_loc_ids = [loc for loc in loc_ids if pd.notna(loc)]
    if not valid_loc_ids: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})
    query = f"""
        SELECT LocID, LocName, Lat, Long
        FROM `{BQ_LOCATIONS_TABLE_ID}`
        WHERE LocID IN UNNEST(@loc_ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("loc_ids", "STRING", valid_loc_ids)])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        df['Lat'] = pd.to_numeric(df['Lat'], errors='coerce')
        df['Long'] = pd.to_numeric(df['Long'], errors='coerce')
        df['LocID'] = df['LocID'].astype(str)
        return df
    except Exception as e:
        st.error(f"Error fetching location data: {e}", icon="☁️")
        print(traceback.format_exc())
        return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

@st.cache_data(ttl=1800)
def load_bigquery_forecast(_client):
    if not _client: return None
    query = f"SELECT * FROM `{BQ_FORECAST_TABLE_ID}` ORDER BY date DESC"
    try:
        df = _client.query(query).to_dataframe(create_bqstorage_client=True)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        for col in ['forecast_value', 'actual_value', 'lower_bound', 'upper_bound']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(subset=['date'], inplace=True)
        return df
    except Exception as e:
        st.error(f"Error loading forecast data from BigQuery table `{BQ_FORECAST_TABLE_ID}`: {e}", icon="☁️")
        print(traceback.format_exc())
        return None

def clean_and_validate_inventory(df):
    if df is None: return None
    df_cleaned = df.copy()
    required_cols = ['Quantity', 'Demand (Required)']
    numeric_cols = ['Price (USD)', 'Quantity', 'Discount (%)', 'Demand (Required)']
    missing_req = [col for col in required_cols if col not in df_cleaned.columns]
    if missing_req:
        st.error(f"Inventory Error: Missing required columns after BQ load/rename: {', '.join(missing_req)}", icon="❗")
        st.caption(f"Check the `load_bigquery_inventory` function and the column names in table `{BQ_PRODUCTS_TABLE_ID}`.")
        return None
    rows_with_numeric_issues = 0
    for col in numeric_cols:
        if col in df_cleaned.columns:
            if not pd.api.types.is_numeric_dtype(df_cleaned[col]):
                initial_nulls = df_cleaned[col].isnull().sum()
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                final_nulls = df_cleaned[col].isnull().sum()
                if final_nulls > initial_nulls:
                    rows_with_numeric_issues += (final_nulls - initial_nulls)
    if rows_with_numeric_issues > 0:
        st.warning(f"Inventory Warning: {rows_with_numeric_issues} non-numeric values found in numeric columns and were ignored.", icon="⚠️")
    initial_rows = len(df_cleaned)
    df_cleaned.dropna(subset=required_cols, inplace=True)
    rows_dropped = initial_rows - len(df_cleaned)
    if rows_dropped > 0:
        st.warning(f"Inventory Warning: {rows_dropped} rows removed due to missing or invalid required values ('Quantity', 'Demand (Required)').", icon="⚠️")
    if df_cleaned.empty:
        st.error("Inventory Error: No valid data remaining after cleaning.", icon="❗")
        return None
    try:
        df_cleaned['Quantity'] = df_cleaned['Quantity'].astype(pd.Int64Dtype())
        df_cleaned['Demand (Required)'] = df_cleaned['Demand (Required)'].astype(pd.Int64Dtype())
    except Exception as e:
        st.warning(f"Could not ensure Quantity/Demand are integer types (using {df_cleaned['Quantity'].dtype}): {e}", icon="ℹ️")
    return df_cleaned

def highlight_demand(row):
    demand = pd.to_numeric(row.get('Demand (Required)'), errors='coerce')
    quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
    num_cols = len(row)
    default_style = ['background-color: none'] * num_cols
    if pd.isna(demand) or pd.isna(quantity):
        return default_style
    try:
        if demand > quantity:
            return ['background-color: #fee2e2'] * num_cols
        elif demand == quantity:
            return ['background-color: #ffedd5'] * num_cols
        else:
            return ['background-color: #dcfce7'] * num_cols
    except TypeError:
        print(f"Warning: Type error comparing demand ({demand}, type {type(demand)}) and quantity ({quantity}, type {type(quantity)})")
        return default_style

@st.cache_data(ttl=3600)
def get_osrm_route(points_df):
    if points_df.shape[0] < 2:
        st.warning("Need at least two points to generate a route.", icon="📍")
        return None
    if not all(col in points_df.columns for col in ['Long', 'Lat']):
        st.error("Missing 'Long' or 'Lat' columns in the points data for OSRM.", icon="❌")
        return None
    valid_points = points_df.dropna(subset=['Long', 'Lat'])
    if valid_points.shape[0] < 2:
        st.warning("Not enough valid coordinate pairs after dropping NaNs.", icon="📍")
        return None
    locs_str = ";".join([f"{lon},{lat}" for lon, lat in valid_points[['Long', 'Lat']].values])
    osrm_base_url = "http://router.project-osrm.org/route/v1/driving/"
    request_url = f"{osrm_base_url}{locs_str}?overview=full&geometries=polyline"
    try:
        response = requests.get(request_url, timeout=20)
        response.raise_for_status()
        route_data = response.json()
        if route_data.get('code') == 'Ok' and route_data.get('routes'):
            encoded_polyline = route_data['routes'][0]['geometry']
            decoded_coords_lat_lon = polyline.decode(encoded_polyline)
            route_path_lon_lat = [[lon, lat] for lat, lon in decoded_coords_lat_lon]
            st.success("Road directions obtained from OSRM.", icon="🗺️")
            return route_path_lon_lat
        else:
            st.warning(f"OSRM could not find a route: {route_data.get('message', 'No details provided.')}", icon="✖️")
            return None
    except requests.exceptions.Timeout:
        st.error("Error calling OSRM API: Request timed out. The demo server might be busy or unreachable.", icon="⏱️")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Error calling OSRM API: {e}", icon="🌐")
        return None
    except Exception as e:
        st.error(f"Error processing OSRM response: {e}", icon="⚙️")
        print(traceback.format_exc())
        return None

# --- Helper Functions from chatbot.py ---
def create_distance_matrix(addresses: List[str], api_key: str) -> Optional[List[List[int]]]:
    logger.info(f"Fetching distance matrix for {len(addresses)} addresses.")
    try:
        max_elements = 100; num_addresses = len(addresses)
        if num_addresses == 0: logger.error("No addresses."); return None
        max_rows = max_elements // num_addresses if num_addresses > 0 else max_elements
        if max_rows == 0: max_rows = 1
        q, r = divmod(num_addresses, max_rows); distance_matrix = []
        for i in range(q):
            origin_addresses = addresses[i * max_rows : (i + 1) * max_rows]
            response = send_request(origin_addresses, addresses, api_key)
            if not response: logger.error(f"Send request failed chunk {i}."); return None
            built_matrix = build_distance_matrix(response)
            if built_matrix is None: logger.error(f"Build matrix failed chunk {i}."); return None
            distance_matrix.extend(built_matrix)
        if r > 0:
            origin_addresses = addresses[q * max_rows : q * max_rows + r]
            response = send_request(origin_addresses, addresses, api_key)
            if not response: logger.error("Send request failed remainder."); return None
            built_matrix = build_distance_matrix(response)
            if built_matrix is None: logger.error("Build matrix failed remainder."); return None
            distance_matrix.extend(built_matrix)
        logger.info("Successfully built distance matrix.")
        if not distance_matrix or len(distance_matrix) != num_addresses or not all(len(row) == num_addresses for row in distance_matrix):
            logger.error(f"Final matrix invalid shape. Got {len(distance_matrix)}x{[len(r) for r in distance_matrix if r]}")
            return None
        return distance_matrix
    except Exception as e: logger.error(f"Error creating distance matrix: {e}", exc_info=True); return None

def send_request(origin_addresses: List[str], dest_addresses: List[str], api_key: str) -> Optional[Dict]:
    def build_address_str(addr): return '|'.join(addr)
    try:
        base = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial'
        origins = urllib.parse.quote(build_address_str(origin_addresses)); dests = urllib.parse.quote(build_address_str(dest_addresses))
        url = f"{base}&origins={origins}&destinations={dests}&key={api_key}"
        with urllib.request.urlopen(url, timeout=20) as resp: data = json.loads(resp.read())
        if data.get('status') != 'OK': logger.error(f"Dist Matrix API Error: {data.get('status')}, {data.get('error_message', 'N/A')}"); return None
        return data
    except Exception as e: logger.error(f"Error sending Dist Matrix req: {e}", exc_info=True); return None

def build_distance_matrix(response: Dict) -> Optional[List[List[int]]]:
    matrix = [];
    if not response or 'rows' not in response: return None
    try:
        expected_cols = len(response.get('destination_addresses', []))
        for i, row in enumerate(response['rows']):
            row_list = []; elements = row.get('elements', [])
            if len(elements) != expected_cols: logger.error(f"Row {i} bad len."); return None
            for j, elem in enumerate(elements):
                status = elem.get('status'); dist = elem.get('distance')
                if status == 'OK' and dist and 'value' in dist: row_list.append(dist['value'])
                else: logger.warning(f"Elem ({i},{j}) status {status}"); row_list.append(9999999)
            matrix.append(row_list)
        return matrix
    except Exception as e: logger.error(f"Error building dist matrix row: {e}", exc_info=True); return None

def format_solution(data: Dict, manager: pywrapcp.RoutingIndexManager, routing: pywrapcp.RoutingModel, solution: pywrapcp.Assignment, week_no: int) -> List[Dict[str, Any]]:
    routes_data = []; logger.info("Formatting solution.")
    try:
        total_dist = 0; dist_dim = routing.GetDimensionOrDie("Distance")
        for v_id in range(data["num_vehicles"]):
            if not routing.IsVehicleUsed(solution, v_id): continue
            idx = routing.Start(v_id); rider = f"Rider{v_id + 1}"; seq = 1
            while not routing.IsEnd(idx):
                node_idx = manager.IndexToNode(idx); rec_id = f"ROUTE_REC_{week_no}_{rider}_{seq}"
                routes_data.append({"RouteRecordID": rec_id, "WeekNo": week_no, "RiderID": rider, "Seq": seq, "LocID": node_idx})
                idx = solution.Value(routing.NextVar(idx)); seq += 1
                if routing.IsEnd(idx):
                    end_node = manager.IndexToNode(idx); end_rec_id = f"ROUTE_REC_{week_no}_{rider}_{seq}"
                    routes_data.append({"RouteRecordID": end_rec_id, "WeekNo": week_no, "RiderID": rider, "Seq": seq, "LocID": end_node})
            end_node_idx = routing.End(v_id); route_dist = solution.Value(dist_dim.CumulVar(end_node_idx))
            logger.info(f"Formatted route {rider}: Seq {seq}, Dist {route_dist}m"); total_dist += route_dist
        logger.info(f"Total dist: {total_dist}m"); return routes_data
    except Exception as e: logger.error(f"Error formatting solution: {e}", exc_info=True); return []

def generate_routes_tool_internal(num_vehicles: int, week_no: int) -> Dict[str, Any]:
    logger.info(f"Generating routes Week {week_no}, {num_vehicles} vehicles.")
    result = {"status": "error", "message": "Route generation failed.", "routes_data": None, "objective_value": None, "max_route_distance": None}
    if not isinstance(num_vehicles, int) or num_vehicles <= 0: result["message"] = "Num vehicles invalid."; return result
    if not isinstance(week_no, int) or week_no <= 0: result["message"] = "Week number invalid."; return result
    if not GOOGLE_MAPS_API_KEY or "YOUR_GOOGLE_MAPS_API_KEY" in GOOGLE_MAPS_API_KEY: result["message"] = "Maps API key not set."; return result
    addresses = BASE_ADDRESSES;
    if len(addresses) <= num_vehicles: result["message"] = f"Not enough addresses ({len(addresses)})."; return result
    distance_matrix = create_distance_matrix(addresses, GOOGLE_MAPS_API_KEY)
    if distance_matrix is None: result["message"] = "Failed to create distance matrix."; return result
    data = {"distance_matrix": distance_matrix, "num_vehicles": num_vehicles, "depot": 0}
    try:
        manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), num_vehicles, data["depot"])
        routing = pywrapcp.RoutingModel(manager)
        def dist_cb(f,t): n_f=manager.IndexToNode(f); n_t=manager.IndexToNode(t); return data["distance_matrix"][n_f][n_t]
        t_idx = routing.RegisterTransitCallback(dist_cb); routing.SetArcCostEvaluatorOfAllVehicles(t_idx)
        dim = "Distance"; routing.AddDimension(t_idx, 0, 3000000, True, dim); dist_dim = routing.GetDimensionOrDie(dim); dist_dim.SetGlobalSpanCostCoefficient(100)
        params = pywrapcp.DefaultRoutingSearchParameters(); params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH; params.time_limit.FromSeconds(30)
        logger.info("Solving..."); solution = routing.SolveWithParameters(params)
    except Exception as e: logger.error(f"OR-Tools error: {e}", exc_info=True); result["message"] = f"OR-Tools error: {e}"; return result
    if solution:
        logger.info("Solution found.")
        formatted_routes = format_solution(data, manager, routing, solution, week_no)
        if not formatted_routes: result["message"] = "Failed to format solution."; return result
        result["status"] = "success"; result["message"] = f"Generated routes Week {week_no}, {num_vehicles} vehicles."
        result["routes_data"] = formatted_routes; result["objective_value"] = solution.ObjectiveValue()
        max_dist = 0
        for v_id in range(num_vehicles):
            if routing.IsVehicleUsed(solution, v_id):
                try: end_idx = routing.End(v_id); r_dist = solution.Value(dist_dim.CumulVar(end_idx)); max_dist = max(r_dist, max_dist)
                except Exception as e: logger.warning(f"No cumul dist for {v_id}: {e}")
        result["max_route_distance"] = max_dist; logger.info(f"Obj: {result['objective_value']}, MaxDist: {max_dist}m")
    else:
        stat_map = {pywrapcp.ROUTING_NOT_SOLVED:"NOT_SOLVED",pywrapcp.ROUTING_FAIL:"FAIL", pywrapcp.ROUTING_FAIL_TIMEOUT:"TIMEOUT", pywrapcp.ROUTING_INVALID:"INVALID"}; solver_stat = routing.status()
        stat_str = stat_map.get(solver_stat, f"UNKNOWN_{solver_stat}"); result["message"] = f"OR-Tools no solution. Status: {stat_str}."; logger.warning(result["message"])
    return result

def insert_routes_to_bigquery_tool_internal(routes_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    table_id = BQ_ROUTES_TABLE_ID
    logger.info(f"Attempting BQ insert: {len(routes_data)} rows into {table_id}")
    result = {"status": "error", "message": "BQ insert failed.", "rows_inserted": 0, "errors": None}
    if not isinstance(routes_data, list): result["message"] = "Invalid input type."; return result
    if not routes_data: result["message"] = "No data to insert."; result["status"] = "success"; return result
    if not table_id or "your-gcp-project" in table_id: result["message"] = "Routes BQ table ID not set."; return result
    try:
        client = get_bq_client()
        if client:
            errors = client.insert_rows_json(table_id, routes_data, skip_invalid_rows=False)
            if not errors: result["status"] = "success"; result["message"] = f"Inserted {len(routes_data)} rows into {table_id}."; result["rows_inserted"] = len(routes_data)
            else: result["message"] = "Errors during BQ insert."; result["errors"] = errors; logger.error(f"BQ Insert Errors: {[f'Row {e['index']}: {e['errors']}' for e in errors]}")
    except Exception as e: result["message"] = f"BQ insertion error: {e}"; logger.error(result["message"], exc_info=True); result["errors"]=[str(e)]
    return result

def update_quantity_in_bigquery(project_id: str, dataset_id: str, table_name: str) -> Dict[str, Union[str, int, None]]:
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"Attempting quantity update for table: {full_table_id}")
    result_status = {"status": "error", "message": "Update failed.", "affected_rows": None}
    if not all([project_id, dataset_id, table_name]):
        result_status["message"] = "Missing project_id, dataset_id, or table_name."
        logger.error(result_status["message"])
        return result_status
    try:
        client = get_bq_client()
        if not client:
            raise Exception("BigQuery client initialization failed.")
    except Exception as e:
        msg = f"Failed to initialize BigQuery client: {e}"
        logger.error(msg, exc_info=True)
        result_status["message"] = msg
        return result_status
    sql_query = f"""
        UPDATE `{full_table_id}`
        SET Quantity = `Demand__Required_`
        WHERE `Demand__Required_` > Quantity;
    """
    logger.info(f"Executing replenishment SQL: {sql_query}")
    try:
        query_job = client.query(sql_query)
        query_job.result()
        affected_rows = query_job.num_dml_affected_rows
        msg = f"Successfully updated Quantity in table: {full_table_id}. Affected rows: {affected_rows}"
        logger.info(msg)
        result_status["status"] = "success"
        result_status["message"] = msg
        result_status["affected_rows"] = affected_rows
    except NotFound:
        msg = f"Error: Table '{full_table_id}' not found."
        logger.error(msg)
        result_status["message"] = msg
    except GoogleAPIError as e:
        msg = f"An error occurred during BigQuery update: {e}"
        logger.error(msg, exc_info=True)
        result_status["message"] = msg
    except Exception as e:
        msg = f"An unexpected error occurred: {e}"
        logger.error(msg, exc_info=True)
        result_status["message"] = msg
    return result_status

def get_date_range(user_prompt):
    nl_sql_model = GenerativeModel(NL_BQ_AGENT_MODEL_ID)
    today = datetime.date.today(); day_of_week_name = today.strftime("%A"); today_str = today.strftime("%Y-%m-%d")
    example_json = f'{{ "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}}'
    prompt = f"""Analyze request: '{user_prompt}'. Determine start/end date. Today: {today_str} ({day_of_week_name}). Consider 'last week', 'this month'. Output ONLY JSON: {example_json}"""
    try:
        response = nl_sql_model.generate_content(prompt).candidates[0].content.parts[0].text
        response = response.strip().replace("```json", "").replace("```", "").strip(); json.loads(response)
        logger.info(f"Date range determined: {response}"); return response
    except Exception as e: logger.error(f"Failed get date range: {e}. Using today."); return f'{{"start_date": "{today_str}", "end_date": "{today_str}"}}'

def parse_and_format_dates(date_range_json_str: str) -> Dict[str, str]:
    try:
        data_dict = json.loads(date_range_json_str)
        if 'start_date' not in data_dict or 'end_date' not in data_dict: raise ValueError("Missing keys")
        start_date = pd.to_datetime(data_dict['start_date']).strftime('%Y-%m-%d')
        end_date = pd.to_datetime(data_dict['end_date']).strftime('%Y-%m-%d')
        return {"start_date": start_date, "end_date": end_date}
    except Exception as e:
        logger.error(f"Failed parse/format date JSON '{date_range_json_str}': {e}")
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        return {"start_date": today_str, "end_date": today_str}

list_calendar_dates_func = FunctionDeclaration(name="get_parsed_date_range", description=f"Parses date refs relative to today ({datetime.date.today()}) -> structured dates.", parameters={"type": "object", "properties": {"date_phrase": {"type": "string", "description": "NL phrase for date range (e.g., 'last week')."}}, "required": ["date_phrase"]})
list_datasets_func = FunctionDeclaration(name="list_datasets", description="List available BQ datasets.", parameters={"type": "object", "properties": {}})
list_tables_func = FunctionDeclaration(name="list_tables", description=f"List tables in '{BQ_DATASET_ID}'.", parameters={"type": "object", "properties": {"dataset_id": {"type": "string", "description": f"Dataset ID (default {BQ_DATASET_ID})."}}})
get_table_func = FunctionDeclaration(name="get_table", description="Get schema/desc for a table.", parameters={"type": "object", "properties": {"table_name": {"type": "string", "description": f"Table name in '{BQ_DATASET_ID}'."}}, "required": ["table_name"]})
sql_query_func = FunctionDeclaration(name="sql_query", description="Execute BQ SQL query.", parameters={"type": "object", "properties": {"query": {"type": "string", "description": f"BQ SQL query for '{PROJECT_ID}.{BQ_DATASET_ID}'."}}, "required": ["query"]})
nl_sql_tool = Tool(function_declarations=[list_calendar_dates_func, list_datasets_func, list_tables_func, get_table_func, sql_query_func])

def answer_query_from_bq_internal(user_query: str) -> str:
    logger.info(f"NL->SQL: Processing query: '{user_query}'")
    available_tables = ["product_inventory", "routes", "order_table"]
    try:
        model = GenerativeModel(NL_BQ_AGENT_MODEL_ID, tools=[nl_sql_tool])
        chat = model.start_chat(response_validation=False)
    except Exception as e: logger.error(f"NL->SQL: Failed init Gemini: {e}", exc_info=True); return f"Error: Could not init AI model ({e})."
    prompt = f"""Analyze user question: '{user_query}'. Answer using data from BQ dataset '{BQ_DATASET_ID}'.
    Available tables in this dataset: {', '.join(available_tables)}.
    Steps: 1. Parse dates (`get_parsed_date_range` if needed). 2. ID relevant tables from the list above (`get_table` for schema if needed). 3. Construct SQL query for `{PROJECT_ID}.{BQ_DATASET_ID}`. 4. Execute (`sql_query`). 5. Summarize result concisely in natural language (no SQL/table names). If no data, say so. If query fails, report error. Do NOT perform updates or replenishment."""
    try:
        response = chat.send_message(prompt); part = response.candidates[0].content.parts[0]
        while hasattr(part, 'function_call') and part.function_call and part.function_call.name:
            function_call = part.function_call; api_response = None; params = {key: value for key, value in function_call.args.items()}
            logger.info(f"NL->SQL: LLM call: {function_call.name} params: {params}")
            try:
                if function_call.name == "get_parsed_date_range": api_response = parse_and_format_dates(get_date_range(params["date_phrase"]))
                elif function_call.name == "list_datasets": api_response = [BQ_DATASET_ID]
                elif function_call.name == "list_tables": api_response = available_tables
                elif function_call.name == "get_table":
                    table_name = params["table_name"]
                    if table_name not in available_tables:
                        api_response = f"Error: Access denied. Can only query schema for tables: {', '.join(available_tables)}."
                    else:
                        full_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
                        try: table_info = get_bq_client().get_table(full_table_id); api_response = {"description": table_info.description, "schema": [{"name": f.name, "type": f.field_type} for f in table_info.schema]}
                        except NotFound: api_response = f"Error: Table '{full_table_id}' not found."
                        except Exception as e_get: api_response = f"Error getting table {full_table_id}: {e_get}"
                elif function_call.name == "sql_query":
                    query = params["query"]; logger.info(f"NL->SQL: Executing BQ: {query}")
                    if re.search(r'\b(UPDATE|DELETE|INSERT|MERGE|CREATE|DROP|ALTER)\b', query, re.IGNORECASE):
                        logger.warning(f"NL->SQL: Denied potentially harmful query: {query}")
                        api_response = {"error": "Query denied. This tool can only run SELECT queries."}
                    else:
                        try:
                            bq_client = get_bq_client()
                            if bq_client:
                                query_job = bq_client.query(query); results_list = [dict(row.items()) for row in query_job.result()]
                                MAX_ROWS_TO_LLM = 50
                                if len(results_list) > MAX_ROWS_TO_LLM: logger.warning(f"Truncating BQ results {len(results_list)}->{MAX_ROWS_TO_LLM}"); api_response = {"results": results_list[:MAX_ROWS_TO_LLM], "truncated": True, "total_rows_found": len(results_list)}
                                else: api_response = {"results": results_list, "truncated": False}
                            else:
                                api_response = {"error": "BigQuery client not available."}
                        except Forbidden as e_sql:
                            logger.error(f"NL->SQL: BQ query permission denied: {e_sql}", exc_info=True)
                            api_response = {"error": f"Permission denied for BQ query. Check service account permissions for table access."}
                        except GoogleAPIError as e_sql:
                            logger.error(f"NL->SQL: BQ API query failed: {e_sql}", exc_info=True)
                            api_response = {"error": f"BQ query failed: {e_sql.message}"}
                        except Exception as e_sql:
                            logger.error(f"NL->SQL: BQ query unexpected error: {e_sql}", exc_info=True)
                            api_response = {"error": f"BQ query failed with unexpected error: {e_sql}"}
                else: api_response = {"error": f"Unknown func call: {function_call.name}"}
            except Exception as e_func: logger.error(f"NL->SQL: Error exec func {function_call.name}: {e_func}", exc_info=True); api_response = {"error": f"Failed exec tool {function_call.name}: {e_func}"}
            logger.info(f"NL->SQL: Resp to LLM for {function_call.name}: {str(api_response)[:500]}...")
            response = chat.send_message(Part.from_function_response(name=function_call.name, response={"content": api_response}))
            if response.candidates and response.candidates[0].content.parts: part = response.candidates[0].content.parts[0]
            else: logger.error("NL->SQL: Empty response from LLM after func call."); return "Error: Empty response from AI."
        if hasattr(part, 'text') and part.text: final_response = part.text
        elif hasattr(response, 'text') and response.text: final_response = response.text
        else: logger.error("NL->SQL: Loop ended but no text content."); final_response = "Error: Could not extract final answer."
        logger.info(f"NL->SQL: Final summary: {final_response}"); return final_response
    except Exception as e: logger.error(f"NL->SQL: Error during chat: {e}", exc_info=True); return f"Error processing query ({e})."

def generate_routes_wrapper(num_vehicles: int, week_no: int) -> Union[str, List[Dict[str, Any]]]:
    logger.info(f"Agent Tool: generate_routes_wrapper called for Week {week_no}, {num_vehicles} vehicles.")
    if not isinstance(num_vehicles, int) or num_vehicles <= 0: return "Tool Input Error: num_vehicles must be a positive integer."
    if not isinstance(week_no, int) or week_no <= 0: return "Tool Input Error: week_no must be a positive integer."
    gen_result = generate_routes_tool_internal(num_vehicles=num_vehicles, week_no=week_no)
    if gen_result.get("status") != "success" or not gen_result.get("routes_data"):
        error_msg = gen_result.get("message", "Route generation failed."); logger.error(f"Gen failed: {error_msg}"); return f"Route Generation Error: {error_msg}"
    generated_routes = gen_result["routes_data"]
    logger.info(f"Generated {len(generated_routes)} route steps successfully.")
    logger.info("Attempting auto-insert of generated routes to BigQuery...")
    insert_result = insert_routes_to_bigquery_tool_internal(routes_data=generated_routes)
    if insert_result.get("status") != "success":
        insert_error_msg = insert_result.get("message", "Saving to BQ failed."); logger.error(f"Insert failed after gen: {insert_error_msg}")
        return f"Warning: Routes generated ({len(generated_routes)} steps), but failed to save to BigQuery: {insert_error_msg}. Data: {generated_routes}"
    else:
        success_msg = insert_result.get("message", f"Successfully generated and saved {len(generated_routes)} route steps.")
        logger.info(f"Generated and saved routes: {success_msg}")
        return generated_routes

def answer_query_from_bq_wrapper(user_query: str) -> str:
    logger.info(f"Agent Tool: answer_query_from_bq_wrapper query: '{user_query}'")
    if not user_query or not isinstance(user_query, str): return "Tool Input Error: Invalid or empty query provided."
    return answer_query_from_bq_internal(user_query=user_query)

def run_inventory_replenishment_wrapper() -> str:
    logger.info(f"Agent Tool: run_inventory_replenishment_wrapper called.")
    try:
        result = update_quantity_in_bigquery(project_id=PROJECT_ID, dataset_id=BQ_DATASET_ID, table_name="product_inventory")
        message = result.get("message", "Replenishment status unknown.")
        if result.get("status") == "success" and result.get("affected_rows") is not None:
            message = "That the orders for Inventory replenishment has been created and Inventory is updated"
        elif result.get("status") == "error":
            message = f"Replenishment Error: {message}"
        logger.info(f"Replenishment result message: {message}")
        return message
    except Exception as e:
        logger.error(f"Error calling inventory replenishment tool: {e}", exc_info=True)
        return f"Tool Execution Error: Failed to run inventory replenishment due to: {e}"

def create_route_generator_agent():
    logger.info("Creating Route Generator & Saver Agent instance...")
    return Agent(
        name="Route Generation Agent",
        role="Generate vehicle routes for a given week and number of riders, and automatically save them to BigQuery.",
        model=AgnoGemini(id=ROUTE_AGENT_MODEL_ID),
        instructions=[
            "1. Identify the **Week Number** (integer > 0) and **Number of Riders/Vehicles** (integer > 0) from the user request.",
            "2. If any information is missing, ask the user for clarification.",
            "3. Once you have both numbers, execute the `generate_routes_wrapper` tool with `num_vehicles` and `week_no`.",
            "4. This tool performs BOTH route generation AND saving to BigQuery.",
            "5. The tool returns either a LIST of route dictionaries (on success) or a STRING (on error or warning).",
            "6. **If the tool returns a LIST:** Report success, stating clearly that routes were 'generated and saved'. Mention the number of route steps (length of the list). Display the key information for each step (e.g., RiderID, Seq, LocID) in a readable format (like a table or formatted list).",
            "7. **If the tool returns a STRING:** Report the message exactly as received. This could be an error message or a warning (e.g., if saving failed after generation).",
            "8. Do NOT ask to save the data, it's automatic.",
            "9. Do NOT query data or perform inventory updates."
        ],
        tools=[generate_routes_wrapper],
        show_tool_calls=True,
        markdown=True,
    )

def create_nl_bigquery_agent():
    logger.info("Creating NL BigQuery Agent instance...")
    return Agent(
        name="BigQuery NL Query Agent",
        role=f"Answer questions about supply chain data (orders, inventory, routes, etc.) by querying the BigQuery dataset '{BQ_DATASET_ID}'. Handles greetings.",
        model=AgnoGemini(id=NL_BQ_AGENT_MODEL_ID),
        instructions=[
            "1. If the user asks a question about existing supply chain data (like orders, product inventory levels, past routes, counts, summaries): Execute the `answer_query_from_bq_wrapper` tool.",
            "2. Pass the **entire user's question** as the `user_query` parameter to the tool.",
            "3. The tool will handle converting the question to SQL, querying BigQuery, and summarizing the result.",
            "4. Present the final text response provided by the tool directly to the user.",
            "5. Do NOT attempt to generate new routes or save any data.",
            "6. Do NOT attempt to run inventory replenishment or updates.",
            "7. If the user asks about generating routes or replenishing inventory, politely state you cannot do that, but the 'Route Generation Agent' or 'Inventory Replenishment Agent' can.",
            "8. Handle simple greetings like 'Hello' or 'Hi'."
        ],
        tools=[answer_query_from_bq_wrapper],
        show_tool_calls=True,
        markdown=True,
    )

def create_replenish_agent():
    logger.info("Creating Inventory Replenishment Agent instance...")
    return Agent(
        name="Inventory Replenishment Agent",
        role=f"Updates inventory quantities in the '{REPLENISH_TABLE_ID}' table based on demand.",
        model=AgnoGemini(id=REPLENISH_AGENT_MODEL_ID),
        instructions=[
            f"1. Your sole purpose is to trigger inventory replenishment in the BigQuery table `{REPLENISH_TABLE_ID}`.",
            "2. When requested to replenish inventory, run replenishment, or update stock based on demand, execute the `run_inventory_replenishment_wrapper` tool.",
            "3. This tool takes **no arguments**.",
            "4. The tool performs the update: `SET Quantity = Demand__Required_ WHERE Demand__Required_ > Quantity`.",
            "5. Strictly return the message That the orders for Inventory replenishment has been created and Inventory is updated ",
            "6. Report this status message directly to the user.",
            "7. Do NOT ask for table names, column names, or any other parameters.",
            "8. Do NOT generate routes or query data.",
            
        ],
        tools=[run_inventory_replenishment_wrapper],
        show_tool_calls=True,
        markdown=True,
    )

def create_router_team():
    logger.info("Creating Router Team with NL->SQL and Replenishment agents...")
    route_gen_save_agent = create_route_generator_agent()
    nl_bq_agent = create_nl_bigquery_agent()
    replenish_agent = create_replenish_agent()
    return Team(
        name="Supply Chain Task Router",
        mode="route",
        model=AgnoGemini(id=TEAM_ROUTER_MODEL_ID),
        members=[route_gen_save_agent, nl_bq_agent, replenish_agent],
        show_tool_calls=False,
        markdown=True,
        instructions=[
            "Route the user's request to the most appropriate agent based on keywords and intent:",
            "1. **Generate/Create/Plan/Make Routes:** -> 'Route Generation Agent'.",
            "2. **Query/Read/Show/Find/Count/Ask about data (orders, inventory, past routes):** -> 'BigQuery NL Query Agent'.",
            "3. **Replenish/Update Inventory/Stock based on demand:** -> 'Inventory Replenishment Agent'.",
            "4. **Save/Insert Data:** -> If related to routes, inform the user route generation handles saving automatically and route to 'Route Generation Agent'. Otherwise, clarify the request or route to 'BigQuery NL Query Agent' if it's a query about saving.",
            "5. **Greetings/Other:** -> 'BigQuery NL Query Agent'.",
            "Carefully analyze the request and select only ONE agent.",
        ],
        show_members_responses=True
    )

async def get_team_response(team: Team, query: str) -> str:
    logger.info(f"Running query via team: '{query}'")
    try:
        response_object = await team.arun(query)
        response_content = ""
        if isinstance(response_object, str):
            response_content = response_object
        elif hasattr(response_object, 'content'):
            content_data = response_object.content
            if content_data is None:
                response_content = "Agent executed but returned no content."
                logger.warning(f"Agent response content was None for query: {query}")
            elif isinstance(content_data, str):
                response_content = content_data
            elif isinstance(content_data, list):
                try:
                    if content_data and isinstance(content_data[0], dict):
                        response_content = f"Data:\n```json\n{json.dumps(content_data, indent=None)}\n```"
                    else:
                        response_content = f"Data: {str(content_data)}"
                except Exception as json_e:
                    logger.warning(f"Could not JSON format list response: {json_e}")
                    response_content = f"Data: {str(content_data)}"
            else:
                try: response_content = str(content_data);
                except Exception: response_content = "Error processing agent response content type."
        elif response_object is None:
            response_content = "No response generated by the team."
            logger.warning(f"Team returned None for query: {query}")
        else:
            try:
                response_content = f"Received unexpected response type: {type(response_object).__name__}. Content: {str(response_object)}"
            except Exception:
                logger.error(f"Failed converting unknown response type: {type(response_object)} to string.")
                response_content = "Error processing AI response object."
        if not response_content:
            response_content = "Received empty response from the agent."
            logger.warning(f"Empty final response content for query: {query}")
        log_response_preview = (response_content[:500] + '...') if len(response_content) > 500 else response_content
        logger.info(f"Team final response preview: {log_response_preview}")
        return response_content
    except ModelProviderError as e:
        logger.error(f"Model Provider Error: {e.message}", exc_info=False)
        status_msg = f" (Status Code: {e.status_code})" if hasattr(e, 'status_code') else ""
        return f"AI model communication error: {e.message}{status_msg}. Please check model availability and configuration."
    except Forbidden as e:
        logger.error(f"Permission Error during team execution query='{query}': {e}", exc_info=True)
        return f"Permission Error: Access denied during request processing. Check GCP service account permissions for BigQuery/Vertex AI. Details: {e}"
    except Exception as e:
        logger.error(f"Error during team execution query='{query}': {e}", exc_info=True)
        if "pydantic_core._pydantic_core.ValidationError" in str(e):
            logger.error(f"Pydantic Validation Error likely during tool result processing: {e}", exc_info=True)
            return "Internal Error: Failed processing tool result (validation failed). Check agent instructions and tool output format compatibility. See logs for details."
        return f"Error processing your request: {type(e).__name__}. Please check the application logs for more details."

# --- Streamlit App Configuration ---
st.set_page_config(page_title="Supply Chain Operations Hub", page_icon="🚚", layout="wide")
st.markdown(APP_STYLE, unsafe_allow_html=True)

# --- Sidebar Selector ---
selected_section = st.sidebar.selectbox("Select Section", ["Dashboard", "Chatbot"])

# --- Dashboard Section ---
if selected_section == "Dashboard":
    bq_client = get_bq_client()
    with st.spinner("Loading order data from Excel..."):
        df_orders_raw = load_excel(ORDER_EXCEL_PATH, "Orders")
    with st.spinner("Loading inventory data from BigQuery..."):
        df_inventory_bq_raw = load_bigquery_inventory(bq_client)
    with st.spinner("Loading historical demand data from CSV..."):
        df_history_demand = load_historical_demand_data()
    with st.spinner("Loading forecast data from BigQuery..."):
        df_forecast_demand = load_bigquery_forecast(bq_client)
    with st.spinner("Processing inventory data..."):
        df_inventory_cleaned = clean_and_validate_inventory(df_inventory_bq_raw)

    df_orders = None
    df_orders_loaded_successfully = False
    load_error_message = ""
    if df_orders_raw is not None:
        with st.spinner("Processing order data..."):
            try:
                df_orders = df_orders_raw.copy()
                if 'Order Date' in df_orders.columns:
                    df_orders['Order Date'] = pd.to_datetime(df_orders['Order Date'], errors='coerce')
                    initial_rows = len(df_orders)
                    df_orders.dropna(subset=['Order Date'], inplace=True)
                    if len(df_orders) < initial_rows:
                        st.warning(f"Removed {initial_rows - len(df_orders)} orders with invalid dates.", icon="⚠️")
                else:
                    st.warning("Order data is missing the 'Order Date' column. Date filtering will not work.", icon="⚠️")
                price_cols_to_convert = ['Unit Price (USD)', 'Total Price (USD)']
                for col in price_cols_to_convert:
                    if col in df_orders.columns:
                        if not pd.api.types.is_numeric_dtype(df_orders[col]):
                            initial_nulls = df_orders[col].isnull().sum()
                            df_orders[col] = pd.to_numeric(df_orders[col], errors='coerce')
                            new_nulls = df_orders[col].isnull().sum() - initial_nulls
                            if new_nulls > 0:
                                st.warning(f"{new_nulls} non-numeric values found in '{col}' were ignored.", icon="⚠️")
                            df_orders[col].fillna(0, inplace=True)
                    else:
                        st.warning(f"Order data is missing the '{col}' column.", icon="⚠️")
                if 'Order Status' not in df_orders.columns:
                    st.error("Order data is missing the 'Order Status' column. Cannot calculate status metrics.", icon="❗")
                    df_orders_loaded_successfully = False
                elif 'Product Name' not in df_orders.columns:
                    st.error("Order data is missing the 'Product Name' column. Product filtering disabled.", icon="❗")
                    df_orders_loaded_successfully = True
                elif df_orders.empty:
                    st.warning(f"Order file ({os.path.basename(ORDER_EXCEL_PATH)}) contained no valid data after processing.", icon="📄")
                    df_orders_loaded_successfully = True
                else:
                    df_orders_loaded_successfully = True
            except Exception as e:
                st.error(f"Error processing order data: {e}", icon="❌")
                load_error_message = str(e)
                df_orders = None
                df_orders_loaded_successfully = False
    else:
        st.info("Order Management data could not be loaded.", icon="ℹ️")
        st.caption(f"Expected file: `{ORDER_EXCEL_PATH}`")
        df_orders_loaded_successfully = False

    st.markdown("""
    <style>
    .custom-header-container {
        background-color:  #1a73e8;
        padding: 1rem 1rem;
        border-radius: 8px;
        border-left: 6px solid #1a73e8;
        border-right: 6px solid #1a73e8;
        margin-bottom: 1rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .custom-header-container h1 {
        color: #ffffff;
        margin-bottom: 0.5rem;
        font-size: 900;
        font-weight: 600;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    .custom-header-container h1 img {
        margin-right: 15px;
        height: 60px;
    }
    .custom-header-container p {
        color: #ffffff;
        font-size: 1rem;
        margin-bottom: 0;
    }
    </style>
    """, unsafe_allow_html=True)

    icon_url = "https://media-hosting.imagekit.io/d4d2d070da764e7a/supply-chain%20(1).png?Expires=1838385562&Key-Pair-Id=K2ZIVPTIP2VGHC&Signature=rI6qlVGN1aOU6B2kLFPU~ZPYiyXFC8eEqvDp~Tnjf9-XnMk2GI~9QYhtG9yS1n12nQ~Xg9H5UCw-uByoFNwmMbAZhvoQYrQAmREiud-IzIQKBncPOB9XVmOxnDCGBvXd6xmC7z~eJV~cjrmaqXqUL4tRVYQQ330kNVuI3Qg2MB9DbjeYuPiHGsqGTOPSDBQw8~Upmcf2oB3whSq-7Fg5R~LYSLmSRFPAalm2Anlw8fxbiCbeVp0yZy6uGG2YSnZ5BSFDHEPL2E4MsYRYL-2HySHoTflBe3D2fJJGQsiIKp8QnZ8UQE0toJaxIZCgTjfrwhtpbL-V3DI4YQ3Jdwxo5w__"
    st.markdown(f"""
    <div class="custom-header-container">
        <h1><img src="{icon_url}" alt="Logistics Icon"> Supply Chain Intelligence Hub</h1>
        <p>Drive efficiency with integrated Sales Forecasting, Inventory Management, Order Management, and Route Optimization.</p>
    </div>
    """, unsafe_allow_html=True)

    tab_demand, tab_inventory, tab_orders, tab_route = st.tabs(["📈 Sales Forecast", "📦 Inventory", "🛒 Orders", "🗺️ Rider Route"])

    with tab_demand:
        st.markdown('<div class="forecast-tab-content">', unsafe_allow_html=True)
        st.markdown('<h2 class="tab-header">Sales Forecast</h2>', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">Historical Data</h3>', unsafe_allow_html=True)
        if df_history_demand is not None:
            if not df_history_demand.empty:
                st.dataframe(df_history_demand, use_container_width=True)
            else:
                st.info(f"Historical demand file (`{os.path.basename(HISTORY_CSV_PATH)}`) was loaded but is empty.", icon="📄")
        else:
            st.warning(f"Could not load or process historical demand from CSV.", icon="⚠️")
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">Forecast Data</h3>', unsafe_allow_html=True)
        if bq_client is None:
            st.warning("BigQuery connection unavailable, cannot load forecast data.", icon="☁️")
        elif df_forecast_demand is not None:
            if not df_forecast_demand.empty:
                st.dataframe(df_forecast_demand, use_container_width=True)
            else:
                st.info(f"Forecast data table (`{BQ_FORECAST_TABLE_ID}`) is empty or returned no results.", icon="📄")
        else:
            st.warning("Could not load forecast data from BigQuery.", icon="☁️")
        st.markdown('</div>', unsafe_allow_html=True)


        # --- Function to Fetch Data from BigQuery ---
        @st.cache_data(ttl=3600)  # Cache data for 1 hour to reduce API calls
        def load_forecast_data():
            client = bigquery.Client(project=g_PROJECT_ID)
            query = f"""
                SELECT date, Product_Code, predicted
                FROM `{g_PROJECT_ID}.{g_DATASET_ID}.{g_TABLE_ID}`
                ORDER BY date
            """
            df = client.query(query).to_dataframe()
            df['date'] = pd.to_datetime(df['date'])
            return df
        g_PROJECT_ID = "gebu-data-ml-day0-01-333910"
        g_DATASET_ID = "supply_chain"
        g_TABLE_ID = "forecast1"
        # --- Main Streamlit Application ---
        st.title("Product Forecast Visualization")

        # Load the data
        forecast_df = load_forecast_data()

        # Get unique product codes for the dropdown
        product_codes = forecast_df['Product_Code'].unique()

        # Create the product selection dropdown
        selected_product = st.selectbox("Select a Product:", product_codes)

        # Filter data for the selected product
        filtered_df = forecast_df[forecast_df['Product_Code'] == selected_product].copy()

        if not filtered_df.empty:
            # Find the latest date in the filtered data
            latest_date = filtered_df['date'].max()

            # Generate the next ten days
            future_dates = [latest_date + timedelta(days=i) for i in range(1, 11)]
            future_df = pd.DataFrame({'date': future_dates, 'Product_Code': [selected_product] * 10, 'predicted': [None] * 10})

            # Combine historical and future data for plotting
            plot_df = pd.concat([filtered_df, future_df], ignore_index=True)

            # Create the forecast plot using Plotly
            fig = px.line(plot_df, x='date', y='predicted',
                        title=f"Forecast for Product: {selected_product}",
                        labels={'predicted': 'Predicted Value', 'date': 'Date'})

            # Highlight the historical data
            # Convert datetime to milliseconds since epoch for Plotly
            latest_date_ms = latest_date.to_pydatetime().timestamp() * 1000
            fig.add_vline(x=latest_date_ms, line_dash="dash", line_color="grey",
                        annotation_text="", annotation_position="top right")

            # Update x-axis tick format to display dates nicely
            fig.update_xaxes(tickformat="%Y-%m-%d")

            # Display the plot in Streamlit
            st.plotly_chart(fig)

    with tab_inventory:
        st.markdown('<h2 class="tab-header">Inventory Management</h2>', unsafe_allow_html=True)
        if df_inventory_cleaned is not None and not df_inventory_cleaned.empty:
            total_items = len(df_inventory_cleaned)
            quantities = pd.to_numeric(df_inventory_cleaned['Quantity'], errors='coerce')
            demands = pd.to_numeric(df_inventory_cleaned['Demand (Required)'], errors='coerce')
            valid_comparison_mask = quantities.notna() & demands.notna()
            valid_quantities = quantities[valid_comparison_mask]
            valid_demands = demands[valid_comparison_mask]
            shortages = (valid_demands > valid_quantities).sum()
            exact_match = (valid_demands == valid_quantities).sum()
            surplus = (valid_demands < valid_quantities).sum()
            st.markdown('<div class="card-container">', unsafe_allow_html=True)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f'<div class="info-card"><span class="card-label">Total SKUs</span><span class="card-value">{total_items}</span></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="warning-card"><span class="card-label">Potential Shortages</span><span class="card-value">{shortages}</span></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="neutral-card"><span class="card-label">Exact Demand Match</span><span class="card-value">{exact_match}</span></div>', unsafe_allow_html=True)
            with col4:
                st.markdown(f'<div class="success-card"><span class="card-label">Surplus Stock</span><span class="card-value">{surplus}</span></div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            st.markdown("""
            <div class="legend-container">
                <span class="legend-title">Inventory Key:</span>
                <span class="legend-item legend-red"><span class="legend-color-box"></span>Shortage (Demand > Quantity)</span>
                <span class="legend-item legend-orange"><span class="legend-color-box"></span>Exact Match (Demand == Quantity)</span>
                <span class="legend-item legend-green"><span class="legend-color-box"></span>Surplus (Demand < Quantity)</span>
            </div>
            """, unsafe_allow_html=True)
            st.dataframe(df_inventory_cleaned.style.apply(highlight_demand, axis=1), use_container_width=True, hide_index=True)
            st.caption(f"Data loaded and processed from BigQuery table: `{BQ_PRODUCTS_TABLE_ID}`")
        elif df_inventory_bq_raw is not None:
            st.warning("Inventory data loaded from BigQuery but could not be processed or is empty after cleaning.", icon="⚠️")
            st.caption(f"Source table: `{BQ_PRODUCTS_TABLE_ID}`. Check cleaning logic and data quality.")
        elif bq_client is None:
            st.error("Inventory data unavailable: Could not connect to BigQuery.", icon="☁️")
        else:
            st.error(f"Inventory data could not be loaded from BigQuery table `{BQ_PRODUCTS_TABLE_ID}`.", icon="❌")
            st.caption("Check BigQuery connection, table name, and permissions.")

    with tab_orders:
        st.markdown('<h2 class="tab-header">Order Management</h2>', unsafe_allow_html=True)
        if not df_orders_loaded_successfully:
            st.error(f"Error loading or processing order data from `{ORDER_EXCEL_PATH}`.", icon="🚨")
            if 'load_error_message' in locals() and load_error_message:
                st.error(f"Details: {load_error_message}")
            st.caption(f"Please ensure the file exists, is accessible, and contains valid data (especially 'Order Status', 'Total Price (USD)').")
        elif df_orders is None or df_orders.empty:
            st.warning(f"Order Management file (`{os.path.basename(ORDER_EXCEL_PATH)}`) loaded but is empty or contains no valid orders after processing.", icon="📄")
        else:
            total_orders = len(df_orders)
            total_order_value = 0
            if 'Total Price (USD)' in df_orders.columns:
                total_order_value = df_orders['Total Price (USD)'].sum()
            else:
                st.caption("Metric 'Total Value' unavailable (missing 'Total Price (USD)' column).")
            shipped_orders = 0
            delivered_orders = 0
            pending_orders = 0
            processing_orders = 0
            status_metrics_available = False
            if 'Order Status' in df_orders.columns:
                status_counts = df_orders['Order Status'].value_counts()
                shipped_orders = status_counts.get('Shipped', 0)
                delivered_orders = status_counts.get('Delivered', 0)
                pending_orders = status_counts.get('Pending', 0)
                processing_orders = status_counts.get('Processing', 0)
                status_metrics_available = True
            else:
                st.caption("Status metrics unavailable (missing 'Order Status' column).")
            st.markdown('<div class="card-container">', unsafe_allow_html=True)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f'<div class="info-card"><span class="card-label">Total Orders</span><span class="card-value">{total_orders}</span></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="success-card"><span class="card-label">Delivered</span><span class="card-value">{delivered_orders}</span></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="neutral-card"><span class="card-label">Pending</span><span class="card-value">{pending_orders}</span></div>', unsafe_allow_html=True)
            with col4:
                st.markdown(f'<div class="success-card"><span class="card-label">Total Value (USD)</span><span class="card-value">${total_order_value:,.2f}</span></div>', unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            st.markdown("<h4>Order Details</h4>", unsafe_allow_html=True)
            st.dataframe(df_orders, use_container_width=True, hide_index=True)

    with tab_route:
        st.markdown('<h2 class="tab-header">Rider Route</h2>', unsafe_allow_html=True)
        if bq_client is None:
            st.warning("BigQuery connection unavailable. Route visualization disabled.", icon="☁️")
        else:
            weeks_riders_df = get_available_weeks_riders(bq_client)
            if weeks_riders_df.empty:
                st.warning("Could not load available Weeks/Riders from BigQuery.", icon="⚠️")
                selected_week = None
                selected_rider = None
            else:
                col_select1, col_select2 = st.columns(2)
                with col_select1:
                    available_weeks = sorted(weeks_riders_df['WeekNo'].dropna().unique().astype(int), reverse=True)
                    if not available_weeks:
                        st.warning("No weeks found in the route data.")
                        selected_week = None
                    else:
                        selected_week = st.selectbox("Select Week:", available_weeks, index=0, key="route_week_selector", help="Select the week number for the route.")
                with col_select2:
                    selected_rider = None
                    if selected_week is not None:
                        riders_in_week = sorted(weeks_riders_df[weeks_riders_df['WeekNo'] == selected_week]['RiderID'].dropna().unique())
                        if not riders_in_week:
                            st.warning(f"No riders found for Week {selected_week}.")
                        else:
                            selected_rider = st.selectbox("Select Rider:", riders_in_week, key="route_rider_selector", help="Select the rider ID for the route.")
                    else:
                        st.info("Select a week to see available riders.")
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            if selected_week is not None and selected_rider:
                st.markdown(f"#### Route Map: Week {selected_week}, Rider {selected_rider}")
                with st.spinner(f"Loading route for W{selected_week}, R{selected_rider}..."):
                    rider_route_df = get_route_data(bq_client, selected_week, selected_rider)
                if rider_route_df.empty or 'LocID' not in rider_route_df.columns:
                    st.warning("No route sequence data found for this selection.", icon="📍")
                else:
                    unique_loc_ids = rider_route_df['LocID'].dropna().unique().tolist()
                    if not unique_loc_ids:
                        st.warning("Route data exists but contains no valid Location IDs.", icon="🤨")
                    else:
                        with st.spinner("Loading location details..."):
                            locations_df = get_location_data(bq_client, unique_loc_ids)
                        if locations_df.empty:
                            st.error(f"Could not find location details for the route stops.", icon="❌")
                        else:
                            route_details_df = pd.merge(rider_route_df.sort_values(by='Seq'), locations_df, on='LocID', how='left')
                            missing_coords = route_details_df['Lat'].isnull().sum() + route_details_df['Long'].isnull().sum()
                            if missing_coords > 0:
                                missing_locs = route_details_df[route_details_df['Lat'].isnull() | route_details_df['Long'].isnull()]['LocID'].nunique()
                                st.warning(f"{missing_locs} locations in the route are missing coordinates and will be excluded from the map.", icon="⚠️")
                                route_details_df.dropna(subset=['Lat', 'Long'], inplace=True)
                            if route_details_df.empty:
                                st.warning("No locations with valid coordinates found for this route.", icon="🙁")
                            else:
                                with st.spinner("Fetching road directions from OSRM..."):
                                    actual_route_path = get_osrm_route(route_details_df[['Long', 'Lat']])
                                path_layer_data = None
                                path_color = [255, 165, 0, 180]
                                if actual_route_path:
                                    path_layer_data = pd.DataFrame({'path': [actual_route_path]})
                                    path_color = [0, 128, 255, 200]
                                elif route_details_df.shape[0] >= 2:
                                    st.info("Could not fetch road directions. Drawing straight lines between stops.")
                                    straight_line_path = route_details_df[['Long', 'Lat']].values.tolist()
                                    path_layer_data = pd.DataFrame({'path': [straight_line_path]})
                                else:
                                    st.info("Only one valid point, cannot draw a path.")
                                def get_icon_data(loc_id, seq, max_seq, min_seq):
                                    base_scale = 1.0
                                    is_dc = (str(loc_id) == DC_LOC_ID)
                                    is_start = (seq == min_seq)
                                    is_end = (seq == max_seq)
                                    icon_url = DC_PIN_URL if is_dc else STORE_PIN_URL
                                    size_multiplier = 1.6 if is_dc and (is_start or is_end) else (1.3 if is_dc else 1.0)
                                    return {
                                        "url": icon_url,
                                        "width": int(PIN_WIDTH * size_multiplier * base_scale),
                                        "height": int(PIN_HEIGHT * size_multiplier * base_scale),
                                        "anchorY": int(PIN_HEIGHT * size_multiplier * base_scale * PIN_ANCHOR_Y_FACTOR),
                                    }
                                min_sequence = route_details_df['Seq'].min()
                                max_sequence = route_details_df['Seq'].max()
                                route_details_df['icon_data'] = route_details_df.apply(lambda row: get_icon_data(row['LocID'], row['Seq'], max_sequence, min_sequence), axis=1)
                                try:
                                    initial_latitude = route_details_df['Lat'].mean()
                                    initial_longitude = route_details_df['Long'].mean()
                                    initial_view_state = pdk.ViewState(latitude=initial_latitude, longitude=initial_longitude, zoom=11, pitch=45, bearing=0)
                                except Exception:
                                    initial_view_state = pdk.ViewState(latitude=35.1495, longitude=-90.0490, zoom=10, pitch=30)
                                layers = []
                                if path_layer_data is not None:
                                    path_layer = pdk.Layer("PathLayer", data=path_layer_data, get_path="path", get_color=path_color, width_min_pixels=4, pickable=False)
                                    layers.append(path_layer)
                                icon_layer = pdk.Layer("IconLayer", data=route_details_df, get_icon="icon_data", get_position=["Long", "Lat"], get_size='icon_data.height', size_scale=1, pickable=True, auto_highlight=True, highlight_color=[255, 255, 0, 180])
                                layers.append(icon_layer)
                                tooltip = {
                                    "html": """
                                    <div style='background-color: rgba(0,0,0,0.7); color: white; padding: 8px 12px; border-radius: 5px; font-family: sans-serif; font-size: 0.9em;'>
                                        <b>{LocName}</b><br/>
                                        Stop #: {Seq}<br/>
                                        ID: {LocID}<br/>
                                        Coords: {Lat:.4f}, {Long:.4f}
                                    </div>
                                    """,
                                    "style": {"backgroundColor": "rgba(0,0,0,0)", "color": "white"}
                                }
                                st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/light-v10", initial_view_state=initial_view_state, layers=layers, tooltip=tooltip), use_container_width=True)
                                st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
                                st.subheader("Route Summary")
                                summary_items = []
                                first_loc_id = route_details_df.iloc[0]['LocID'] if not route_details_df.empty else None
                                last_loc_id = route_details_df.iloc[-1]['LocID'] if not route_details_df.empty else None
                                first_seq = route_details_df['Seq'].min()
                                last_seq = route_details_df['Seq'].max()
                                is_first_dc = str(first_loc_id) == DC_LOC_ID if first_loc_id is not None else False
                                stop_counter = 0
                                for index, row in route_details_df.iterrows():
                                    loc_name = row['LocName']
                                    loc_id = row['LocID']
                                    seq = row['Seq']
                                    prefix = ""
                                    icon = "📍"
                                    is_current_dc = (str(loc_id) == DC_LOC_ID)
                                    if seq == first_seq:
                                        if is_current_dc:
                                            prefix = f"**Start (DC):** "
                                            icon = "🏭"
                                        else:
                                            stop_counter += 1
                                            prefix = f"**Start (Stop {stop_counter}):** "
                                            icon = "🏁"
                                    elif seq == last_seq:
                                        if is_current_dc:
                                            if is_first_dc and first_seq == last_seq:
                                                prefix = f"**Start & End (DC):** "
                                            elif is_first_dc:
                                                prefix = f"**End (Return DC):** "
                                            else:
                                                prefix = f"**End (DC):** "
                                            icon = "🏭"
                                        else:
                                            stop_counter += 1
                                            prefix = f"**End (Stop {stop_counter}):** "
                                            icon = "🏁"
                                    else:
                                        if is_current_dc:
                                            prefix = f"**Via DC:** "
                                            icon = "🏭"
                                        else:
                                            stop_counter += 1
                                            prefix = f"**Stop {stop_counter}:** "
                                    summary_items.append(f"* {icon} {prefix} {loc_name} (`{loc_id}`)")
                                st.markdown("\n".join(summary_items))
                                st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
                                st.markdown("#### Route Stop Details")
                                st.dataframe(route_details_df[['Seq', 'LocID', 'LocName', 'Lat', 'Long']].reset_index(drop=True), use_container_width=True, hide_index=True)
            elif selected_week is None or selected_rider is None:
                st.info("Select a Week and Rider above to view the route details and map.", icon="👆")

# --- Chatbot Section ---
elif selected_section == "Chatbot":
    try:
        if not PROJECT_ID or "your-gcp-project" in PROJECT_ID:
            raise ValueError("GCP Project ID is not set correctly in the Configuration section.")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)
        logger.info(f"Vertex AI Initialized. Project: {PROJECT_ID}, Location: {LOCATION}")
    except google_auth_exceptions.DefaultCredentialsError as e:
        logger.error("Google Cloud Authentication Failed. Ensure ADC or GOOGLE_APPLICATION_CREDENTIALS is set.", exc_info=True)
        st.error("Google Cloud Authentication Failed. Please authenticate your environment (e.g., `gcloud auth application-default login`).")
        st.stop()
    except Exception as e:
        logger.error(f"Failed to initialize Vertex AI SDK: {e}", exc_info=True)
        st.error(f"Initialization Error: {e}. Check Project ID/Location and permissions.")
        st.stop()

    st.title("🚚 Supply Chain Bot 📦")
    st.caption(f"Generate routes, query data, or replenish inventory | Models: Various `{MODEL_ID}` | Dataset: `{BQ_DATASET_ID}`")
    st.markdown("**Warning:** API keys might be visible in configuration if not using secrets management.")

    with st.sidebar:
        st.subheader("Examples:")
        st.markdown("- `Generate routes for week 20 using 2 riders`")
        st.markdown("- `How many orders have been delivered?`")
        st.markdown("- `Show me the inventory quantity available for Product_0123`")
        st.markdown("- `What were the routes for Rider1 in week 19?`")
        st.markdown("- `Replenish inventory based on demand`")
        # st.subheader("Info:")
        # st.markdown(f"**Project:** `{PROJECT_ID}`")
        # st.markdown(f"**Dataset:** `{BQ_DATASET_ID}`")
        # st.markdown(f"**Maps API Key:** `...{GOOGLE_MAPS_API_KEY[-4:]}`")
        # st.markdown(f"**Routes Table:** `{BQ_ROUTES_TABLE_ID.split('.')[-1]}`")
        # st.markdown(f"**Replenish Table:** `product_inventory`")

    if "messages" not in st.session_state:
        st.session_state.messages = [{"role": "assistant", "content": "How can I help with routes, data queries, or inventory replenishment today?"}]

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    if prompt := st.chat_input("Ask to generate routes, query data, or replenish inventory..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"): st.write(prompt)
        with st.chat_message("assistant"):
            message_placeholder = st.empty(); message_placeholder.markdown("Thinking...")
            logger.info(f"User prompt: {prompt}")
            try:
                team_router = create_router_team()
                assistant_response = asyncio.run(get_team_response(team_router, prompt))
                message_placeholder.write(assistant_response)
                st.session_state.messages.append({"role": "assistant", "content": assistant_response})
            except Exception as e:
                logger.error(f"Fatal error creating/running team: {e}", exc_info=True)
                error_msg = f"An unexpected application error occurred ({type(e).__name__}). Please contact support or check the logs."
                message_placeholder.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})

# --- Main Execution Guard ---
if __name__ == "__main__":
    if not GOOGLE_MAPS_API_KEY or "YOUR_GOOGLE_MAPS_API_KEY" in GOOGLE_MAPS_API_KEY: st.error("FATAL: Google Maps API Key missing/not set in Configuration."); st.stop()
    if not BQ_ROUTES_TABLE_ID or "your-gcp-project" in BQ_ROUTES_TABLE_ID: st.error("FATAL: Route BQ Table ID missing/not set in Configuration."); st.stop()
    if not PROJECT_ID or "your-gcp-project" in PROJECT_ID: st.error("FATAL: GCP Project ID missing/not set in Configuration."); st.stop()
    if not BQ_DATASET_ID or "your_bq_dataset" in BQ_DATASET_ID: st.error("FATAL: BQ Dataset ID missing/not set in Configuration."); st.stop()
    if not REPLENISH_TABLE_ID or "your_replenish_table" in REPLENISH_TABLE_ID: st.error(f"FATAL: Replenishment Table ID missing/not set in Configuration."); st.stop()
