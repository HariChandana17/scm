# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import pydeck as pdk
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import numpy as np
import sys
import requests # Import for OSRM API calls
import polyline # Import for decoding polyline geometries
import traceback # For better error details
import base64


# --- Try Importing Necessary Libraries ---
try:
    import openpyxl
    import db_dtypes # Required for BigQuery nullable integers/floats
except ImportError as e:
    missing_lib = str(e).split("'")[-2]
    st.error(f"Error: Missing required library '{missing_lib}'.")
    st.info("Please install required libraries: pip install pandas openpyxl google-cloud-bigquery pydeck db-dtypes requests polyline")
    st.stop()


# --- Page Configuration - MUST BE THE FIRST STREAMLIT COMMAND ---
st.set_page_config(
    page_title="Supply Chain Operations Hub",
    page_icon="🚚", # Changed icon
    layout="wide"
)


# --- File Paths (Ensure these are correct for your environment) ---
# Using relative paths or environment variables is generally safer than absolute paths.
# INVENTORY_EXCEL_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\supply_chain_management\product_data.xlsx" # NO LONGER USED FOR INVENTORY
ORDER_EXCEL_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\Order Management.xlsx"
HISTORY_CSV_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\supply_chain_management\Historical Product Demand.csv"

# --- BigQuery Configuration ---
# !!! IMPORTANT: Replace with your actual Google Cloud Project ID !!!
GCP_PROJECT_ID = "gebu-data-ml-day0-01-333910" 
BQ_DATASET = "supply_chain"
BQ_FORECAST_DATASET = "demand_forecast" # Dataset for forecasts
BQ_LOCATIONS_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.locations"
BQ_ROUTES_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.routes"
# !!! UPDATE with your forecast table name !!! Use the full ID format
BQ_FORECAST_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_FORECAST_DATASET}.forecast1" # <<< UPDATE THIS
# <<< NEW: BigQuery table for Inventory/Products >>>
BQ_PRODUCTS_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_DATASET}.product_inventory" # <<< CHECK IF 'products' IS CORRECT

# --- PyDeck Icon Configuration for Route Map ---
DC_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png" # Using 2x for better resolution
STORE_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png"
PIN_WIDTH = 25 # Base width
PIN_HEIGHT = 41 # Base height
PIN_ANCHOR_Y_FACTOR = 1.0 # Anchor at the bottom

# Assuming DC LocID is exactly 'LOC0'
DC_LOC_ID = 'LOC0'

# ==============================================================================
# Enhanced Styling
# ==============================================================================

APP_STYLE = """
<style>
    /* --- Base & Font --- */
    body {
        font-family: 'Inter', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; /* Modern font stack */
        background-color: #f0f4f8; /* Light grey-blue background */
        color: #333; /* Default text color */
    }

    /* --- Custom Main Header (Replaces .main-header/.main-title) --- */
    .custom-header-container {
        background-color: #ffffff; /* White background for contrast */
        padding: 1.5rem 2rem;
        border-radius: 10px;
        border-left: 6px solid #1a73e8; /* Accent color border (Google Blue) */
        margin-bottom: 2rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.08); /* Subtle shadow */
    }
    .custom-header-container h1 {
        color: #0d47a1; /* Darker blue */
        margin-bottom: 0.5rem;
        font-size: 2.1em; /* Adjusted size */
        font-weight: 700; /* Bolder */
        display: flex;
        align-items: center;
    }
    .custom-header-container h1 img {
        margin-right: 15px; /* Space between icon and text */
        height: 45px; /* Control icon size */
        vertical-align: middle; /* Align icon vertically */
    }
    .custom-header-container p {
        color: #455a64; /* Darker grey text */
        font-size: 1.05rem; /* Slightly larger subtitle */
        margin-bottom: 0;
        line-height: 1.5;
    }

    /* --- Enhanced Tab Styling --- */
    div[data-testid="stTabs"] { /* Target the main tabs container */
        border-bottom: 2px solid #cbd5e1; /* Add a separating line below tabs */
        margin-bottom: 0; /* Remove bottom margin, panel styling will handle space */
    }

    div[data-testid="stTabs"] button[data-baseweb="tab"] { /* Target individual tab buttons */
        /* --- Size & Spacing --- */
        padding: 14px 28px; /* Significantly increase padding */
        margin: 0 5px -2px 0; /* Adjust margin: top/right/bottom/left - negative bottom margin overlaps the border */
        font-size: 1.1em;  /* Increase font size */
        font-weight: 500;  /* Slightly bolder text */
        min-height: 50px; /* Ensure a decent minimum height */
        display: inline-flex; /* Align icon and text nicely */
        align-items: center;
        justify-content: center;

        /* --- Appearance --- */
        color: #475569; /* Default tab text color (slate gray) */
        background-color: #f1f5f9; /* Light background for inactive tabs */
        border: 1px solid #e2e8f0; /* Border around tabs */
        border-bottom: none; /* Remove bottom border initially */
        border-radius: 8px 8px 0 0; /* Round only the top corners */
        transition: background-color 0.2s ease, color 0.2s ease, border-color 0.2s ease, box-shadow 0.2s ease; /* Smooth transitions */
        position: relative; /* Needed for z-index or positioning tricks */
        top: 2px; /* Move inactive tabs down slightly to meet the border */
        cursor: pointer; /* Add pointer cursor */
    }

    /* --- Tab Hover State --- */
    div[data-testid="stTabs"] button[data-baseweb="tab"]:hover {
        background-color: #e2e8f0; /* Slightly darker background on hover */
        color: #1e3a8a; /* Darker blue text on hover */
        border-color: #cbd5e1; /* Match border color on hover */
    }

    /* --- Active Tab State --- */
    div[data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
        background-color: #ffffff; /* White background for active tab (looks connected to content) */
        color: #0061ff; /* Primary blue text */
        font-weight: 600; /* Bolder text for active tab */
        border-color: #cbd5e1; /* Match the container's bottom border color */
        border-bottom: 2px solid #ffffff; /* White bottom border to cover the container line */
        top: 0px; /* Bring active tab to the front (vertically) */
        /* Optional: subtle shadow to lift active tab */
        box-shadow: 0 -2px 4px rgba(0, 0, 0, 0.04);
    }

    /* --- Adjust Tab Panel --- */
    div[data-testid="stTabPanel"] {
        border: 1px solid #cbd5e1; /* Add border to content panel */
        border-top: none; /* Remove top border as tabs handle it */
        padding: 30px 25px; /* Add padding inside the content panel */
        border-radius: 0 0 10px 10px; /* Round bottom corners to match cards */
        margin-top: -2px; /* Pull panel up slightly to meet tabs */
        background-color: #ffffff; /* Ensure content background is white */
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.04); /* Subtle shadow for panel */
        margin-bottom: 30px; /* Add space below the entire tab panel */
    }


    /* --- Tab Headers (Content Headers Inside Tab Panels) --- */
    .tab-header {
        color: #004aad; /* Darker blue */
        font-weight: 700; /* Bolder */
        border-bottom: 4px solid #0061ff; /* Thicker border */
        padding-bottom: 12px; /* Increased padding */
        background-color: #f8f9a;
        margin-top: 0px; /* Reduced top margin as panel has padding */
        margin-bottom: 35px; /* Increased space below header */
        font-size: 1.9em; /* Slightly larger */
        display: flex;
        align-items: center;
        gap: 12px; /* Space between icon and text if icon added */
    }

     /* --- Sub Headers (e.g., Historical/Forecast) --- */
    .sub-header {
        color: #1e293b; /* Dark slate color */
        font-weight: 600; /* Slightly bolder */
        margin-top: 30px; /* Increased space above */
        margin-bottom: 20px; /* Increased space below */
        font-size: 1.5em; /* Larger */
        padding-left: 15px; /* Increased padding */
        background-color: #f8f9fa; /* Very Subtle background highlight */
        padding-top: 8px; /* Add vertical padding */
        padding-bottom: 8px;
        border-radius: 4px; /* Slightly rounded corners */
    }


    /* --- Info Cards --- */
    .card-container { /* This class wraps the columns for inventory cards */
        display: flex; /* Use flexbox for horizontal layout */
        gap: 25px; /* More space between cards */
        margin-bottom: 30px; /* Space below the card row */
        justify-content: space-around; /* Distribute space */
        flex-wrap: wrap; /* Allow wrapping on smaller screens */
    }
    .info-card, .warning-card, .success-card, .neutral-card {
        background-color: #ffffff;
        border: 1px solid #e2e8f0; /* Softer border */
        border-radius: 10px; /* More rounded */
        padding: 25px; /* More padding */
        box-shadow: 0 5px 10px rgba(0, 0, 0, 0.05);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        flex: 1; /* Allow cards to grow and fill space */
        min-width: 180px; /* Minimum width before wrapping */
    }
    .info-card:hover, .warning-card:hover, .success-card:hover, .neutral-card:hover {
        transform: translateY(-5px); /* Slightly more lift */
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.08);
    }
    .card-label {
        font-size: 1em; /* Slightly larger label */
        color: #475569; /* Slate gray */
        margin-bottom: 10px;
        font-weight: 500;
    }
    .card-value {
        font-size: 2.2em; /* Larger value */
        font-weight: 700; /* Bolder value */
    }
    /* Card Accent Colors (using left border and text color) */
    .info-card { border-left: 6px solid #3b82f6; } /* Blue */
    .info-card .card-value { color: #3b82f6; }
    .warning-card { border-left: 6px solid #f59e0b; } /* Amber */
    .warning-card .card-value { color: #f59e0b; }
    .success-card { border-left: 6px solid #10b981; } /* Emerald */
    .success-card .card-value { color: #10b981; }
    .neutral-card { border-left: 6px solid #64748b; } /* Slate */
    .neutral-card .card-value { color: #64748b; }


    /* --- Legend Styling --- */
    .legend-container {
        margin-top: 10px; /* Added top margin */
        margin-bottom: 25px;
        padding: 15px 20px; /* More padding */
        background-color: #eef2f9; /* Lighter background */
        border: 1px solid #dbeafe; /* Light blue border */
        border-radius: 8px;
        display: flex;
        flex-wrap: wrap;
        gap: 20px; /* More space between items */
        align-items: center;
    }
    .legend-title {
        font-weight: 600;
        margin-right: 15px;
        color: #1e3a8a; /* Darker blue */
    }
    .legend-item {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        border-radius: 16px; /* Pill shape */
        font-size: 0.95em;
        border: 1px solid transparent; /* Keep border definition */
    }
    .legend-color-box {
        width: 14px; /* Slightly larger */
        height: 14px;
        margin-right: 10px;
        border-radius: 4px; /* Slightly rounded */
        display: inline-block;
    }
    /* Legend Item Colors (using lighter backgrounds and distinct text colors) */
    .legend-red { background-color: #fee2e2; border-color: #fecaca; color: #b91c1c;}
    .legend-red .legend-color-box { background-color: #ef4444; } /* Red-500 */
    .legend-orange { background-color: #ffedd5; border-color: #fed7aa; color: #c2410c;}
    .legend-orange .legend-color-box { background-color: #f97316; } /* Orange-500 */
    .legend-green { background-color: #dcfce7; border-color: #bbf7d0; color: #15803d;}
    .legend-green .legend-color-box { background-color: #22c55e; } /* Green-500 */


    /* --- Section Spacing --- */
    .section-divider {
        margin-top: 40px;
        margin-bottom: 40px;
        border-top: 1px solid #dde4ed; /* Thicker/more prominent */
    }


    /* --- Enhanced Dataframe Styling --- */
    .stDataFrame {
        margin-top: 15px; /* Reduced top margin as sub-header has more bottom margin */
        margin-bottom: 30px; /* Add space below dataframe */
        border-radius: 8px; /* Slightly less rounded */
        overflow: hidden;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.06); /* Slightly stronger shadow */
        border: 1px solid #d1d9e6; /* Slightly darker border */
    }
     /* Dataframe header */
     .stDataFrame thead th {
        background-color: #f1f5f9; /* Lighter slate blue */
        color: #0f172a; /* Dark text */
        font-weight: 600;
        border-bottom: 2px solid #cbd5e1; /* Stronger bottom border */
        padding: 12px 15px; /* Increase padding */
        text-align: left; /* Ensure left alignment */
        text-transform: uppercase; /* Uppercase headers */
        font-size: 0.85em; /* Slightly smaller header font */
        letter-spacing: 0.5px; /* Add letter spacing */
     }
     /* Dataframe body cells */
     .stDataFrame tbody td {
        padding: 10px 15px; /* Adjust padding */
        border-bottom: 1px solid #e2e8f0; /* Lighter row separator */
        vertical-align: middle; /* Align text vertically */
     }
     /* Zebra striping for rows */
     .stDataFrame tbody tr:nth-child(odd) td {
        background-color: #f8fafc; /* Very light grey for odd rows */
     }
     /* Hover effect for rows */
     .stDataFrame tbody tr:hover td {
        background-color: #eef2f9; /* Light blue hover */
     }

     /* --- Map Styling --- */
    .stPyDeckChart {
        margin-top: 25px;
        border-radius: 10px; /* Match card rounding */
        overflow: hidden; /* Important for border-radius */
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.04);
        border: 1px solid #e2e8f0; /* Subtle border */
        margin-bottom: 25px; /* Add space below map */
    }

    /* --- Footer Caption Style --- */
     .footer-caption {
        text-align: center;
        font-style: italic;
        color: #94a3b8; /* Lighter grey */
        margin-top: 50px;
        border-top: 1px solid #e2e8f0;
        padding-top: 20px;
        font-size: 0.85em;
     }

     /* --- Specific adjustments for Rider Route Tooltip --- */
     .deck-tooltip {
         background-color: rgba(0,0,0,0.75) !important; /* Slightly darker tooltip */
         color: white !important;
         border-radius: 5px !important;
         padding: 8px 12px !important;
         font-size: 0.9em !important;
         box-shadow: 0 2px 5px rgba(0,0,0,0.2); /* Add shadow to tooltip */
     }

    /* --- Specific Container for Forecast Tab (Optional for Scoping - not strictly needed now) --- */
    /* .forecast-tab-content { padding: 10px; } */


</style>
"""

def apply_styling():
    """Applies the custom CSS to the Streamlit app."""
    st.markdown(APP_STYLE, unsafe_allow_html=True)

# ==============================================================================
# BigQuery Client Initialization
# ==============================================================================
@st.cache_resource
def get_bq_client():
    """Initializes and returns a BigQuery client."""
    client = None
    credentials = None
    auth_method = "None"

    # 1. Try Streamlit Secrets (Preferred for deployed apps)
    try:
        if 'gcp_service_account' in st.secrets:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
            client.query("SELECT 1").result() # Test query
            auth_method = "Streamlit Secrets"
            print(f"BigQuery Connection Successful ({auth_method}).")
            return client
    except Exception as e:
        print(f"Connection via Streamlit Secrets failed: {e}")

    # 2. Try Application Default Credentials (ADC) (Good for local dev/cloud envs)
    try:
        if not client:
            client = bigquery.Client(project=GCP_PROJECT_ID)
            client.query("SELECT 1").result() # Test query
            auth_method = "Application Default Credentials (ADC)"
            print(f"BigQuery Connection Successful ({auth_method}).")
            return client
    except Exception as e:
        print(f"Connection via ADC failed: {e}")

    # 3. Try Environment Variable (GOOGLE_APPLICATION_CREDENTIALS)
    try:
        if not client:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
                client.query("SELECT 1").result() # Test query
                auth_method = "GOOGLE_APPLICATION_CREDENTIALS Env Var"
                print(f"BigQuery Connection Successful ({auth_method}).")
                return client
            else:
                 print("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
    except Exception as e:
        print(f"Connection via GOOGLE_APPLICATION_CREDENTIALS failed: {e}")

    # If all methods fail
    print("Fatal: Could not connect to BigQuery using any available method.")
    st.error("Could not connect to Google BigQuery. Please check credentials.", icon="🚨")
    return None

# Initialize client globally
bq_client = get_bq_client()

# ==============================================================================
# Data Loading & Processing Functions
# ==============================================================================

# --- Excel Loading (Only for Orders now) ---
def load_excel(file_path, data_label="Data"):
    """Loads data from an Excel file, strips columns, and handles basic errors."""
    if not os.path.exists(file_path):
         st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
         return None
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = df.columns.str.strip()
        if df.empty: st.warning(f"{data_label} Warning: File is empty: `{os.path.basename(file_path)}`", icon="⚠️")
        return df
    except FileNotFoundError: # Should be caught by os.path.exists, but good to have
         st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
         return None
    except Exception as e:
        st.error(f"An error occurred while reading {data_label} file ({os.path.basename(file_path)}): {e}", icon="❌")
        return None

# --- CSV Loading ---
def load_csv(file_path, data_label="Data"):
    """Loads data from a CSV file and handles basic errors."""
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
         return pd.DataFrame() # Return empty dataframe
    except Exception as e:
        st.error(f"An error occurred while reading {data_label} CSV file ({os.path.basename(file_path)}): {e}", icon="❌")
        return None

@st.cache_data
def load_historical_demand_data():
    """Loads historical demand data from CSV."""
    return load_csv(HISTORY_CSV_PATH, "Historical Demand")


# --- BigQuery Data Fetching (Inventory) --- <<< NEW FUNCTION >>>
@st.cache_data(ttl=1800) # Cache inventory data for 30 mins
def load_bigquery_inventory(_client):
    """Loads inventory data from the BigQuery products table."""
    if not _client:
        st.error("BigQuery client not available. Cannot load inventory data.", icon="☁️")
        return None

    query = f"SELECT * FROM `{BQ_PRODUCTS_TABLE_ID}`"
    try:
        df = _client.query(query).to_dataframe(
            create_bqstorage_client=True,
            # Use db-dtypes for nullable BQ integers/floats
            dtypes={
                "Price__USD_": pd.Float64Dtype(),
                "Quantity": pd.Int64Dtype(),
                "Discount____": pd.Float64Dtype(), # Assuming discount can be float
                "Demand__Required_": pd.Int64Dtype()
            }
        )
        # st.success(f"Successfully loaded inventory data from `{BQ_PRODUCTS_TABLE_ID}`.", icon="✅")

        # --- Rename columns to match expected names in the rest of the app ---
        column_mapping = {
            # 'BQ_Column_Name': 'App_Column_Name'
            'Product_ID': 'Product ID', # Keep space for consistency if needed
            'Product_Name': 'Product Name',
            'Price__USD_': 'Price (USD)',
            'Description': 'Description',
            'Quantity': 'Quantity',
            'Discount____': 'Discount (%)',
            'Country_of_Origin': 'Country of Origin',
            'Demand__Required_': 'Demand (Required)'
            # Add other columns if necessary, remove int64_field_0 if it's just an index
        }

        # Select and rename columns present in the dataframe
        rename_map = {bq_col: app_col for bq_col, app_col in column_mapping.items() if bq_col in df.columns}
        df = df[list(rename_map.keys())].rename(columns=rename_map)

        return df

    except Exception as e:
        st.error(f"Error loading inventory data from BigQuery table `{BQ_PRODUCTS_TABLE_ID}`: {e}", icon="☁️")
        print(traceback.format_exc())
        return None


# --- BigQuery Data Fetching (Route Info) ---
@st.cache_data(ttl=600)
def get_available_weeks_riders(_client):
    if not _client: return pd.DataFrame({'WeekNo': [], 'RiderID': []})
    query = f"SELECT DISTINCT WeekNo, RiderID FROM `{BQ_ROUTES_TABLE}` ORDER BY WeekNo DESC, RiderID ASC"
    try:
        # Use db-dtypes for nullable integers
        df = _client.query(query).to_dataframe(
            create_bqstorage_client=True,
            dtypes={"WeekNo": pd.Int64Dtype()}
        )
        return df
    except Exception as e:
        st.error(f"Error fetching week/rider data from BigQuery: {e}", icon="☁️")
        print(traceback.format_exc()) # Log detailed error
        return pd.DataFrame({'WeekNo': pd.Series(dtype='Int64'), 'RiderID': pd.Series(dtype='str')})

@st.cache_data(ttl=600)
def get_route_data(_client, week: int, rider: str):
    if not _client: return pd.DataFrame({'Seq': [], 'LocID': []})
    # Ensure week is an int, handle potential None or non-int
    try:
        week_int = int(week)
    except (ValueError, TypeError):
        st.error(f"Invalid week number provided: {week}", icon="❌")
        return pd.DataFrame({'Seq': [], 'LocID': []})

    query = f"""
        SELECT Seq, LocID
        FROM `{BQ_ROUTES_TABLE}`
        WHERE WeekNo = @week_no AND RiderID = @rider_id
        ORDER BY Seq ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("week_no", "INT64", week_int),
        bigquery.ScalarQueryParameter("rider_id", "STRING", rider)
    ])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        # Ensure Seq is integer, handle potential non-numeric values gracefully
        if 'Seq' in df.columns:
            df['Seq'] = pd.to_numeric(df['Seq'], errors='coerce').astype(pd.Int64Dtype())
            df.dropna(subset=['Seq'], inplace=True) # Remove rows where Seq couldn't be converted
        return df
    except Exception as e:
        st.error(f"Error fetching route data for W{week}, R{rider}: {e}", icon="☁️")
        print(traceback.format_exc())
        return pd.DataFrame({'Seq': [], 'LocID': []})

@st.cache_data(ttl=3600)
def get_location_data(_client, loc_ids: list):
    if not _client: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})
    if not loc_ids: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

    # Filter out potential None or NaN values from loc_ids
    valid_loc_ids = [loc for loc in loc_ids if pd.notna(loc)]
    if not valid_loc_ids: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

    query = f"""
        SELECT LocID, LocName, Lat, Long
        FROM `{BQ_LOCATIONS_TABLE}`
        WHERE LocID IN UNNEST(@loc_ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("loc_ids", "STRING", valid_loc_ids)
    ])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        # Convert Lat/Long robustly
        df['Lat'] = pd.to_numeric(df['Lat'], errors='coerce')
        df['Long'] = pd.to_numeric(df['Long'], errors='coerce')
        # Keep original LocID types if needed, or ensure string
        df['LocID'] = df['LocID'].astype(str)
        return df
    except Exception as e:
        st.error(f"Error fetching location data: {e}", icon="☁️")
        print(traceback.format_exc())
        return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

# --- BigQuery Data Fetching (Forecast) ---
@st.cache_data(ttl=1800) # Cache forecast data for 30 mins
def load_bigquery_forecast(_client):
    if not _client: return None
    # Use the full table ID defined in constants
    query = f"SELECT * FROM `{BQ_FORECAST_TABLE_ID}` ORDER BY date DESC" # Adjust query as needed
    try:
        df = _client.query(query).to_dataframe(create_bqstorage_client=True)

        # Data type conversions (example, adjust based on your actual schema)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        # Convert potential numeric columns, handling errors
        for col in ['forecast_value', 'actual_value', 'lower_bound', 'upper_bound']: # Example columns
             if col in df.columns:
                 # Use db-dtypes compatible conversion if needed, or standard pandas
                 df[col] = pd.to_numeric(df[col], errors='coerce') # .astype(pd.Float64Dtype()) ?

        # Drop rows where essential columns became NaT/NaN due to conversion errors
        df.dropna(subset=['date'], inplace=True) # Example: Date is essential
        # st.success(f"Successfully loaded forecast data from `{BQ_FORECAST_TABLE_ID}`.", icon="✅")
        return df
    except Exception as e:
        st.error(f"Error loading forecast data from BigQuery table `{BQ_FORECAST_TABLE_ID}`: {e}", icon="☁️")
        print(traceback.format_exc())
        return None

# --- Inventory Cleaning/Highlighting ---
# NOTE: This function now expects the column names defined in the `column_mapping`
# within `load_bigquery_inventory`.
def clean_and_validate_inventory(df):
    """Cleans and validates inventory data (now assumed to come from BQ)."""
    if df is None: return None
    df_cleaned = df.copy()

    # Define essential columns and numeric columns using the *renamed* App column names
    required_cols = ['Quantity', 'Demand (Required)']
    numeric_cols = ['Price (USD)', 'Quantity', 'Discount (%)', 'Demand (Required)']

    # Check for missing required columns (after potential renaming)
    missing_req = [col for col in required_cols if col not in df_cleaned.columns]
    if missing_req:
        st.error(f"Inventory Error: Missing required columns after BQ load/rename: {', '.join(missing_req)}", icon="❗")
        # Also check if the renaming failed in the first place
        st.caption(f"Check the `load_bigquery_inventory` function and the column names in table `{BQ_PRODUCTS_TABLE_ID}`.")
        return None

    # Convert numeric columns, coercing errors and tracking issues
    rows_with_numeric_issues = 0
    for col in numeric_cols:
        if col in df_cleaned.columns:
            # Check if conversion is necessary (BQ might return correct types, but check anyway)
            # BQ nullable ints/floats might come as pandas Float64Dtype/Int64Dtype - these are numeric
            if not pd.api.types.is_numeric_dtype(df_cleaned[col]):
                initial_nulls = df_cleaned[col].isnull().sum()
                # Use pd.to_numeric which handles standard types and potentially object types
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                final_nulls = df_cleaned[col].isnull().sum()
                if final_nulls > initial_nulls:
                     rows_with_numeric_issues += (final_nulls - initial_nulls)
            # Optional: Convert BQ Nullable Ints (Int64Dtype) to float if needed for calculations,
            # or handle potential <NA> values directly in calculations. For simple comparison, it's often fine.
            # Example: If you need float for sure:
            # if pd.api.types.is_integer_dtype(df_cleaned[col]):
            #     df_cleaned[col] = df_cleaned[col].astype(pd.Float64Dtype()) # Or float64 if no NAs expected after dropna

    if rows_with_numeric_issues > 0:
        st.warning(f"Inventory Warning: {rows_with_numeric_issues} non-numeric values found in numeric columns and were ignored.", icon="⚠️")

    # Drop rows with missing values in REQUIRED columns after potential coercion
    initial_rows = len(df_cleaned)
    df_cleaned.dropna(subset=required_cols, inplace=True)
    rows_dropped = initial_rows - len(df_cleaned)

    if rows_dropped > 0:
        st.warning(f"Inventory Warning: {rows_dropped} rows removed due to missing or invalid required values ('Quantity', 'Demand (Required)').", icon="⚠️")

    if df_cleaned.empty:
        st.error("Inventory Error: No valid data remaining after cleaning.", icon="❗")
        return None

    # Ensure Quantity and Demand are appropriate numeric types (Int64Dtype allows NAs, safe choice)
    try:
        # These should already be Int64Dtype from BQ or pd.to_numeric, but explicit conversion is okay
        # If you are SURE there are no NAs after dropna, you could use int, but Int64 is safer.
        df_cleaned['Quantity'] = df_cleaned['Quantity'].astype(pd.Int64Dtype())
        df_cleaned['Demand (Required)'] = df_cleaned['Demand (Required)'].astype(pd.Int64Dtype())
    except Exception as e:
        st.warning(f"Could not ensure Quantity/Demand are integer types (using {df_cleaned['Quantity'].dtype}): {e}", icon="ℹ️")

    return df_cleaned

def highlight_demand(row):
    """Applies background color based on Quantity vs Demand."""
    # Use .get() for safety, in case columns were somehow dropped
    demand = pd.to_numeric(row.get('Demand (Required)'), errors='coerce')
    quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
    num_cols = len(row)

    # Default style if data is missing/invalid (including pandas <NA>)
    default_style = ['background-color: none'] * num_cols

    if pd.isna(demand) or pd.isna(quantity):
        return default_style

    # Apply styles based on comparison
    try:
        if demand > quantity:
            return ['background-color: #fee2e2'] * num_cols # Red
        elif demand == quantity:
            return ['background-color: #ffedd5'] * num_cols # Orange
        else: # demand < quantity
            return ['background-color: #dcfce7'] * num_cols # Green
    except TypeError: # Handle potential comparison errors between types if conversion failed unexpectedly
        print(f"Warning: Type error comparing demand ({demand}, type {type(demand)}) and quantity ({quantity}, type {type(quantity)})")
        return default_style


# --- OSRM Route Fetching Function ---
@st.cache_data(ttl=3600) # Cache OSRM results for an hour
def get_osrm_route(points_df):
    """Gets road route geometry from OSRM for an ordered sequence of points (Long, Lat)."""
    if points_df.shape[0] < 2:
        st.warning("Need at least two points to generate a route.", icon="📍")
        return None # Need at least two points

    # Ensure 'Long' and 'Lat' columns exist
    if not all(col in points_df.columns for col in ['Long', 'Lat']):
         st.error("Missing 'Long' or 'Lat' columns in the points data for OSRM.", icon="❌")
         return None

    # Format coordinates: lon1,lat1;lon2,lat2;... Handle potential NaN values
    valid_points = points_df.dropna(subset=['Long', 'Lat'])
    if valid_points.shape[0] < 2:
        st.warning("Not enough valid coordinate pairs after dropping NaNs.", icon="📍")
        return None

    locs_str = ";".join([f"{lon},{lat}" for lon, lat in valid_points[['Long', 'Lat']].values])
    osrm_base_url = "http://router.project-osrm.org/route/v1/driving/" # Using the public demo server
    # Use overview=full for detailed geometry, geometries=polyline for encoded format
    request_url = f"{osrm_base_url}{locs_str}?overview=full&geometries=polyline"

    try:
        # Increased timeout, OSRM demo can be slow
        response = requests.get(request_url, timeout=20)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        route_data = response.json()

        if route_data.get('code') == 'Ok' and route_data.get('routes'):
            # Decode the geometry of the first (usually best) route
            encoded_polyline = route_data['routes'][0]['geometry']
            # polyline.decode returns list of (lat, lon) tuples
            decoded_coords_lat_lon = polyline.decode(encoded_polyline)
            # Convert to list of [lon, lat] for PyDeck PathLayer
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
    except Exception as e: # Catch potential JSON decoding errors or other issues
        st.error(f"Error processing OSRM response: {e}", icon="⚙️")
        print(traceback.format_exc())
        return None

# ==============================================================================
# Streamlit App Layout
# ==============================================================================

# --- Apply Styling ---
apply_styling()

# --- Load Data ---
# Use spinners to indicate loading activity

# Load Orders from Excel
with st.spinner("Loading order data from Excel..."):
    df_orders_raw = load_excel(ORDER_EXCEL_PATH, "Orders")

# <<< MODIFIED: Load Inventory from BigQuery >>>
with st.spinner("Loading inventory data from BigQuery..."):
    df_inventory_bq_raw = load_bigquery_inventory(bq_client) # Pass client

# Load Historical Demand from CSV
with st.spinner("Loading historical demand data from CSV..."):
    df_history_demand = load_historical_demand_data()

# Load Forecast from BigQuery
with st.spinner("Loading forecast data from BigQuery..."):
    df_forecast_demand = load_bigquery_forecast(bq_client) # Pass client


# --- Process Data ---
# <<< MODIFIED: Process Inventory data fetched from BQ >>>
with st.spinner("Processing inventory data..."):
    # The cleaning function expects the renamed columns
    df_inventory_cleaned = clean_and_validate_inventory(df_inventory_bq_raw)


# Process Orders Data (Remains the same)
df_orders = None
df_orders_loaded_successfully = False
load_error_message = ""

if df_orders_raw is not None:
    with st.spinner("Processing order data..."):
        try:
            df_orders = df_orders_raw.copy()
            # **Important: Convert Order Date to datetime objects**
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


# --- Main Header ---
st.markdown("""
<style>
.custom-header-container {
    background-color:  #1a73e8; /* Light blue-grey background */
    padding: 1rem 1rem;
    border-radius: 8px;
    border-left: 6px solid #1a73e8; /* Accent color border */
    border-right: 6px solid #1a73e8;
    margin-bottom: 1rem;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    text-align: center;
}
.custom-header-container h1 {
    color: #ffffff; /* Darker blue */
    margin-bottom: 0.5rem;
    font-size: 900;
    font-weight: 600; /* Slightly bolder */
    display: flex;
    align-items: center;
    justify-content: center;
}
.custom-header-container h1 img {
    margin-right: 15px; /* Space between icon and text */
    height: 60px; /* Control icon size */
}
.custom-header-container p {
    color: #ffffff; /* Dark grey text */
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


# --- Create Tabs with Icons ---
tab_demand, tab_inventory, tab_orders, tab_route = st.tabs([
    "📈 Sales Forecast",
    "📦 Inventory",
    "🛒 Orders",
    "🗺️ Rider Route"
])


# --- Render Demand Forecast Tab ---
with tab_demand:
    st.markdown('<div class="forecast-tab-content">', unsafe_allow_html=True)
    st.markdown('<h2 class="tab-header">Sales Forecast</h2>', unsafe_allow_html=True)

    # --- Historical Demand Section (from CSV) ---
    st.markdown('<h3 class="sub-header">Historical Data</h3>', unsafe_allow_html=True)
    if df_history_demand is not None:
        if not df_history_demand.empty:
            st.dataframe(df_history_demand, use_container_width=True)
            # st.caption(f"Data loaded from: `{os.path.basename(HISTORY_CSV_PATH)}`")
        else:
            st.info(f"Historical demand file (`{os.path.basename(HISTORY_CSV_PATH)}`) was loaded but is empty.", icon="📄")
    else:
        st.warning(f"Could not load or process historical demand from CSV.", icon="⚠️")

    # --- Forecasted Demand Section (from BigQuery) ---
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.markdown('<h3 class="sub-header">Forecast Data</h3>', unsafe_allow_html=True)
    if bq_client is None:
         st.warning("BigQuery connection unavailable, cannot load forecast data.", icon="☁️")
    elif df_forecast_demand is not None:
        if not df_forecast_demand.empty:
            st.dataframe(df_forecast_demand, use_container_width=True)
            # st.caption(f"Data loaded from BigQuery table: `{BQ_FORECAST_TABLE_ID}`")
        else:
            st.info(f"Forecast data table (`{BQ_FORECAST_TABLE_ID}`) is empty or returned no results.", icon="📄")
    else:
         st.warning("Could not load forecast data from BigQuery.", icon="☁️")

    st.markdown('</div>', unsafe_allow_html=True)


# --- Render Inventory Tab --- <<< MODIFIED >>>
with tab_inventory:
    st.markdown('<h2 class="tab-header">Inventory Management</h2>', unsafe_allow_html=True)

    # Check if data was loaded from BQ AND cleaned successfully
    if df_inventory_cleaned is not None and not df_inventory_cleaned.empty:
        # Calculate metrics AFTER cleaning
        total_items = len(df_inventory_cleaned)

        # Ensure cols are numeric before comparison (should be handled by cleaning, but double-check)
        # Use .astype(float) for comparison to handle potential Int64 <NA> if needed,
        # but direct comparison usually works if types are consistent (Int64 vs Int64).
        # Adding explicit conversion for robustness:
        quantities = pd.to_numeric(df_inventory_cleaned['Quantity'], errors='coerce')
        demands = pd.to_numeric(df_inventory_cleaned['Demand (Required)'], errors='coerce')

        # Perform comparison only where both values are valid numbers
        valid_comparison_mask = quantities.notna() & demands.notna()
        valid_quantities = quantities[valid_comparison_mask]
        valid_demands = demands[valid_comparison_mask]

        shortages = (valid_demands > valid_quantities).sum()
        exact_match = (valid_demands == valid_quantities).sum()
        surplus = (valid_demands < valid_quantities).sum()


        # --- Display Cards Horizontally ---
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
        st.markdown('</div>', unsafe_allow_html=True) # Close card-container

        st.markdown("<br>", unsafe_allow_html=True)

        # --- Legend ---
        st.markdown("""
        <div class="legend-container">
            <span class="legend-title">Inventory Key:</span>
            <span class="legend-item legend-red"><span class="legend-color-box"></span>Shortage (Demand > Quantity)</span>
            <span class="legend-item legend-orange"><span class="legend-color-box"></span>Exact Match (Demand == Quantity)</span>
            <span class="legend-item legend-green"><span class="legend-color-box"></span>Surplus (Demand < Quantity)</span>
        </div>
        """, unsafe_allow_html=True)

        # --- Table ---
        # Display the cleaned dataframe with highlighting
        # Make sure highlight_demand handles potential BQ dtypes (like Int64Dtype)
        st.dataframe(
            df_inventory_cleaned.style.apply(highlight_demand, axis=1),
            use_container_width=True,
            hide_index=True # Hide index for cleaner look
        )
        # <<< MODIFIED CAPTION >>>
        st.caption(f"Data loaded and processed from BigQuery table: `{BQ_PRODUCTS_TABLE_ID}`")

    # <<< MODIFIED ERROR HANDLING >>>
    elif df_inventory_bq_raw is not None: # If raw data loaded from BQ but cleaning failed or resulted in empty df
         st.warning("Inventory data loaded from BigQuery but could not be processed or is empty after cleaning.", icon="⚠️")
         st.caption(f"Source table: `{BQ_PRODUCTS_TABLE_ID}`. Check cleaning logic and data quality.")
    elif bq_client is None: # If BQ client failed to initialize
         st.error("Inventory data unavailable: Could not connect to BigQuery.", icon="☁️")
    else: # If raw data failed to load from BQ for other reasons (e.g., query error, table not found)
        st.error(f"Inventory data could not be loaded from BigQuery table `{BQ_PRODUCTS_TABLE_ID}`.", icon="❌")
        st.caption("Check BigQuery connection, table name, and permissions.")


# --- Render Order Management Tab ---
with tab_orders:
    # Use the existing tab header style
    st.markdown('<h2 class="tab-header">Order Management</h2>', unsafe_allow_html=True)

    if not df_orders_loaded_successfully:
        st.error(f"Error loading or processing order data from `{ORDER_EXCEL_PATH}`.", icon="🚨")
        if 'load_error_message' in locals() and load_error_message: # Check if variable exists and is not empty
             st.error(f"Details: {load_error_message}")
        st.caption(f"Please ensure the file exists, is accessible, and contains valid data (especially 'Order Status', 'Total Price (USD)').")

    elif df_orders is None or df_orders.empty: # Handle case where loading succeeded but df is None or empty
         st.warning(f"Order Management file (`{os.path.basename(ORDER_EXCEL_PATH)}`) loaded but is empty or contains no valid orders after processing.", icon="📄")

    else: # Data loaded successfully and is not empty

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
            processing_orders = status_counts.get('Processing', 0) # Example: add if needed
            status_metrics_available = True
        else:
            st.caption("Status metrics unavailable (missing 'Order Status' column).")

        # --- Display Cards Horizontally using card-container flexbox --
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f'<div class="info-card"><span class="card-label">Total Orders</span><span class="card-value">{total_orders}</span></div>', unsafe_allow_html=True)
        with col2:
            # Choose the most relevant statuses for cards
            st.markdown(f'<div class="success-card"><span class="card-label">Delivered</span><span class="card-value">{delivered_orders}</span></div>', unsafe_allow_html=True)
        with col3:
             st.markdown(f'<div class="neutral-card"><span class="card-label">Pending</span><span class="card-value">{pending_orders}</span></div>', unsafe_allow_html=True)
        with col4:
            st.markdown(f'<div class="success-card"><span class="card-label">Total Value (USD)</span><span class="card-value">${total_order_value:,.2f}</span></div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True) # Close card-container

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Divider
        st.markdown("<h4>Order Details</h4>", unsafe_allow_html=True)

        st.dataframe(df_orders, use_container_width=True, hide_index=True)


# --- Render Rider Route Tab ---
with tab_route:
    st.markdown('<h2 class="tab-header">Rider Route</h2>', unsafe_allow_html=True)

    if bq_client is None:
        st.warning("BigQuery connection unavailable. Route visualization disabled.", icon="☁️")
    else:
        # --- Selection Controls ---
        weeks_riders_df = get_available_weeks_riders(bq_client) # Pass client

        if weeks_riders_df.empty:
            st.warning("Could not load available Weeks/Riders from BigQuery.", icon="⚠️")
            selected_week = None
            selected_rider = None
        else:
            col_select1, col_select2 = st.columns(2)
            with col_select1:
                available_weeks = sorted(
                    weeks_riders_df['WeekNo'].dropna().unique().astype(int),
                    reverse=True
                 )
                if not available_weeks:
                     st.warning("No weeks found in the route data.")
                     selected_week = None
                else:
                    selected_week = st.selectbox(
                        "Select Week:",
                        available_weeks,
                        index=0,
                        key="route_week_selector",
                        help="Select the week number for the route."
                    )

            with col_select2:
                selected_rider = None
                if selected_week is not None:
                    # Filter riders based on selected week *before* unique/sort
                    riders_in_week = sorted(
                        weeks_riders_df[weeks_riders_df['WeekNo'] == selected_week]['RiderID'].dropna().unique()
                    )
                    if not riders_in_week:
                        st.warning(f"No riders found for Week {selected_week}.")
                    else:
                        selected_rider = st.selectbox(
                            "Select Rider:",
                            riders_in_week,
                            key="route_rider_selector",
                            help="Select the rider ID for the route."
                        )
                else:
                    st.info("Select a week to see available riders.")

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

        # --- Map and Details Display ---
        if selected_week is not None and selected_rider:
            st.markdown(f"#### Route Map: Week {selected_week}, Rider {selected_rider}")
            with st.spinner(f"Loading route for W{selected_week}, R{selected_rider}..."):
                rider_route_df = get_route_data(bq_client, selected_week, selected_rider) # Pass client

            if rider_route_df.empty or 'LocID' not in rider_route_df.columns:
                st.warning("No route sequence data found for this selection.", icon="📍")
            else:
                unique_loc_ids = rider_route_df['LocID'].dropna().unique().tolist()
                if not unique_loc_ids:
                    st.warning("Route data exists but contains no valid Location IDs.", icon="🤨")
                else:
                    with st.spinner("Loading location details..."):
                         locations_df = get_location_data(bq_client, unique_loc_ids) # Pass client

                    if locations_df.empty:
                        st.error(f"Could not find location details for the route stops.", icon="❌")
                    else:
                        route_details_df = pd.merge(
                            rider_route_df.sort_values(by='Seq'),
                            locations_df,
                            on='LocID',
                            how='left'
                        )

                        missing_coords = route_details_df['Lat'].isnull().sum() + route_details_df['Long'].isnull().sum()
                        if missing_coords > 0:
                             # Calculate how many unique locations are missing coords
                             missing_locs = route_details_df[route_details_df['Lat'].isnull() | route_details_df['Long'].isnull()]['LocID'].nunique()
                             st.warning(f"{missing_locs} locations in the route are missing coordinates and will be excluded from the map.", icon="⚠️")
                             route_details_df.dropna(subset=['Lat', 'Long'], inplace=True)

                        if route_details_df.empty:
                             st.warning("No locations with valid coordinates found for this route.", icon="🙁")
                        else:
                            with st.spinner("Fetching road directions from OSRM..."):
                                actual_route_path = get_osrm_route(route_details_df[['Long', 'Lat']])

                            path_layer_data = None
                            path_color = [255, 165, 0, 180] # Orange fallback

                            if actual_route_path:
                                path_layer_data = pd.DataFrame({'path': [actual_route_path]})
                                path_color = [0, 128, 255, 200] # Blue for OSRM route
                            elif route_details_df.shape[0] >= 2 :
                                st.info("Could not fetch road directions. Drawing straight lines between stops.")
                                straight_line_path = route_details_df[['Long', 'Lat']].values.tolist()
                                path_layer_data = pd.DataFrame({'path': [straight_line_path]})
                                # Keep orange color
                            else:
                                st.info("Only one valid point, cannot draw a path.")

                            # --- Define Icon Data ---
                            def get_icon_data(loc_id, seq, max_seq, min_seq):
                                base_scale = 1.0
                                is_dc = (str(loc_id) == DC_LOC_ID)
                                is_start = (seq == min_seq) # Use min_seq now
                                is_end = (seq == max_seq)

                                icon_url = DC_PIN_URL if is_dc else STORE_PIN_URL
                                # Slightly larger DC pin, even larger if start/end
                                size_multiplier = 1.6 if is_dc and (is_start or is_end) else (1.3 if is_dc else 1.0)

                                return {
                                    "url": icon_url,
                                    "width": int(PIN_WIDTH * size_multiplier * base_scale),
                                    "height": int(PIN_HEIGHT * size_multiplier * base_scale),
                                    "anchorY": int(PIN_HEIGHT * size_multiplier * base_scale * PIN_ANCHOR_Y_FACTOR),
                                    }

                            min_sequence = route_details_df['Seq'].min() # Calculate min sequence
                            max_sequence = route_details_df['Seq'].max() # Calculate max sequence
                            route_details_df['icon_data'] = route_details_df.apply(
                                lambda row: get_icon_data(row['LocID'], row['Seq'], max_sequence, min_sequence), axis=1
                            )


                            try:
                                initial_latitude = route_details_df['Lat'].mean()
                                initial_longitude = route_details_df['Long'].mean()
                                initial_view_state = pdk.ViewState(
                                    latitude=initial_latitude,
                                    longitude=initial_longitude,
                                    zoom=11,
                                    pitch=45,
                                    bearing=0
                                )
                            except Exception:
                                initial_view_state = pdk.ViewState(
                                    latitude=35.1495, longitude=-90.0490, zoom=10, pitch=30
                                )

                            layers = []
                            if path_layer_data is not None:
                                path_layer = pdk.Layer(
                                    "PathLayer",
                                    data=path_layer_data,
                                    get_path="path",
                                    get_color=path_color,
                                    width_min_pixels=4, # Slightly thinner maybe
                                    pickable=False
                                )
                                layers.append(path_layer)

                            icon_layer = pdk.Layer(
                                "IconLayer",
                                data=route_details_df,
                                get_icon="icon_data",
                                get_position=["Long", "Lat"],
                                get_size='icon_data.height', # Use height for scaling control
                                size_scale=1, # Use direct pixel values
                                pickable=True,
                                auto_highlight=True,
                                highlight_color=[255, 255, 0, 180] # Yellow highlight
                             )
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
                                "style": { # Often ignored by pydeck, rely on CSS class
                                     "backgroundColor": "rgba(0,0,0,0)",
                                     "color": "white"
                                 }
                            }

                            st.pydeck_chart(pdk.Deck(
                                map_style="mapbox://styles/mapbox/light-v10",
                                initial_view_state=initial_view_state,
                                layers=layers,
                                tooltip=tooltip
                            ), use_container_width=True)

                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

                            # --- Route Summary Text ---
                            st.subheader("Route Summary")
                            summary_items = []
                            first_loc_id = route_details_df.iloc[0]['LocID'] if not route_details_df.empty else None
                            last_loc_id = route_details_df.iloc[-1]['LocID'] if not route_details_df.empty else None
                            first_seq = route_details_df['Seq'].min() # Use calculated min
                            last_seq = route_details_df['Seq'].max()  # Use calculated max

                            is_first_dc = str(first_loc_id) == DC_LOC_ID if first_loc_id is not None else False
                            stop_counter = 0 # Counter for non-DC stops

                            for index, row in route_details_df.iterrows():
                                loc_name = row['LocName']
                                loc_id = row['LocID']
                                seq = row['Seq']
                                prefix = ""
                                icon = "📍" # Default stop icon

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
                                         # Check if it's also the start DC (loop)
                                         if is_first_dc and first_seq == last_seq:
                                              prefix = f"**Start & End (DC):** " # Handled by first case? Redundant? No, for single point DC route.
                                         elif is_first_dc: # Started at DC, ended at DC
                                              prefix = f"**End (Return DC):** "
                                         else: # Did not start at DC, but ended at one
                                              prefix = f"**End (DC):** "
                                         icon = "🏭"
                                    else:
                                        stop_counter += 1
                                        prefix = f"**End (Stop {stop_counter}):** "
                                        icon = "🏁"
                                else: # Intermediate stop
                                    if is_current_dc:
                                        # Intermediate DC visit? Less common but possible
                                        prefix = f"**Via DC:** "
                                        icon = "🏭"
                                    else:
                                        stop_counter += 1
                                        prefix = f"**Stop {stop_counter}:** "

                                summary_items.append(f"* {icon} {prefix} {loc_name} (`{loc_id}`)")

                            st.markdown("\n".join(summary_items))

                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

                            # --- Route Details Table ---
                            st.markdown("#### Route Stop Details")
                            st.dataframe(
                                route_details_df[['Seq', 'LocID', 'LocName', 'Lat', 'Long']].reset_index(drop=True),
                                use_container_width=True,
                                hide_index=True
                                )

        elif selected_week is None or selected_rider is None:
            st.info("Select a Week and Rider above to view the route details and map.", icon="👆")


# --- Footer ---
# st.markdown('<p class="footer-caption">Business Operations Dashboard | Enhanced UI Demo</p>', unsafe_allow_html=True)