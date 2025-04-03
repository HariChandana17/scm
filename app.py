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


# --- Try Importing Necessary Libraries ---
try:
    import openpyxl
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

# ==============================================================================
# Configuration Constants
# ==============================================================================

# --- File Paths (Ensure these are correct for your environment) ---
# Using relative paths or environment variables is generally safer than absolute paths.
# For demonstration, keeping the provided absolute paths. Replace if needed.
INVENTORY_EXCEL_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\supply_chain_management\product_data.xlsx"
ORDER_EXCEL_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\Order Management.xlsx"
HISTORY_CSV_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\supply_chain_management\Historical Product Demand.csv"

# --- BigQuery Configuration ---
# !!! IMPORTANT: Replace with your actual Google Cloud Project ID !!!
GCP_PROJECT_ID = "gebu-data-ml-day0-01-333910" # <<< REPLACE WITH YOUR GCP PROJECT ID
BQ_DATASET = "supply_chain"
BQ_FORECAST_DATASET = "demand_forecast" # Dataset for forecasts
BQ_LOCATIONS_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.locations"
BQ_ROUTES_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.routes"
# !!! UPDATE with your forecast table name !!! Use the full ID format
BQ_FORECAST_TABLE_ID = f"{GCP_PROJECT_ID}.{BQ_FORECAST_DATASET}.forecast1" # <<< UPDATE THIS

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
        font-family: 'Inter', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; /* Changed font */
        background-color: #f0f4f8; /* Lighter grey background */
    }

    /* --- Main Header --- */
    .main-header {
        background: linear-gradient(135deg, #0061ff, #004aad); /* Updated gradient */
        padding: 25px 35px;
        border-radius: 12px; /* Softer edges */
        margin-bottom: 35px;
        box-shadow: 0 6px 15px rgba(0, 74, 173, 0.25); /* Slightly deeper shadow */
    }
    .main-title {
        color: white;
        text-align: center;
        font-weight: 700; /* Bolder */
        font-size: 2.5em; /* Larger */
        margin: 0;
        text-shadow: 1px 1px 3px rgba(0, 0, 0, 0.2);
    }

    /* --- Tab Headers (Content Headers Inside Tabs) --- */
    .tab-header {
        color: #004aad; /* Darker blue */
        font-weight: 700; /* Bolder */
        border-bottom: 4px solid #0061ff; /* Thicker border */
        padding-bottom: 10px;
        margin-top: 20px; /* Space above header */
        margin-bottom: 30px; /* Increased space below header */
        font-size: 1.8em; /* Larger */
        display: flex;
        align-items: center;
        gap: 10px; /* Space between icon and text if icon added */
    }

     /* --- Sub Headers (e.g., Historical/Forecast) --- */
    .sub-header {
        color: #1e293b; /* Dark slate color */
        font-weight: 600; /* Slightly bolder */
        margin-top: 25px;
        margin-bottom: 18px;
        font-size: 1.4em;
        border-left: 5px solid #007bff;
        padding-left: 12px;
    }

    /* --- Info Cards --- */
    .card-container { /* This class wraps the columns for inventory cards */
        display: flex; /* Use flexbox for horizontal layout */
        gap: 20px; /* Space between cards */
        margin-bottom: 30px; /* Space below the card row */
        justify-content: space-around; /* Distribute space */
        flex-wrap: wrap; /* Allow wrapping on smaller screens */
    }

    /* Individual card styling */
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
        margin-top: 30px;
        margin-bottom: 30px;
        border-top: 1px solid #e2e8f0; /* Light divider */
    }
    .forecast-section { margin-top: 35px; } /* Space above forecast */

    /* --- Dataframe & Map Styling --- */
    .stDataFrame, .stPyDeckChart {
        margin-top: 25px;
        border-radius: 10px; /* Match card rounding */
        overflow: hidden; /* Important for border-radius */
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.04);
        border: 1px solid #e2e8f0; /* Subtle border */
    }
    /* Try to style dataframe header (might be overridden by Streamlit) */
     .stDataFrame thead th {
        background-color: #f1f5f9; /* Light header */
        color: #0f172a; /* Dark text */
        font-weight: 600;
        border-bottom: 2px solid #e2e8f0;
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

     /* Specific adjustments for Rider Route Tooltip */
     .deck-tooltip {
         background-color: rgba(0,0,0,0.7) !important;
         color: white !important;
         border-radius: 5px !important;
         padding: 8px 12px !important;
         font-size: 0.9em !important;
     }

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

# --- Excel Loading ---
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

@st.cache_data
def load_all_excel_data():
    """Loads both inventory and order Excel files."""
    df_inv_raw = load_excel(INVENTORY_EXCEL_PATH, "Inventory")
    df_ord_raw = load_excel(ORDER_EXCEL_PATH, "Orders")
    return df_inv_raw, df_ord_raw

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
                 df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop rows where essential columns became NaT/NaN due to conversion errors
        df.dropna(subset=['date'], inplace=True) # Example: Date is essential
        st.success(f"Successfully loaded forecast data from `{BQ_FORECAST_TABLE_ID}`.", icon="✅")
        return df
    except Exception as e:
        st.error(f"Error loading forecast data from BigQuery table `{BQ_FORECAST_TABLE_ID}`: {e}", icon="☁️")
        print(traceback.format_exc())
        return None

# --- Inventory Cleaning/Highlighting ---
def clean_and_validate_inventory(df):
    if df is None: return None
    df_cleaned = df.copy()

    # Define essential columns and numeric columns
    required_cols = ['Quantity', 'Demand (Required)']
    numeric_cols = ['Price (USD)', 'Quantity', 'Discount (%)', 'Demand (Required)']

    # Check for missing required columns
    missing_req = [col for col in required_cols if col not in df_cleaned.columns]
    if missing_req:
        st.error(f"Inventory Error: Missing required columns: {', '.join(missing_req)}", icon="❗")
        return None

    # Convert numeric columns, coercing errors and tracking issues
    rows_with_numeric_issues = 0
    for col in numeric_cols:
        if col in df_cleaned.columns:
            # Check if conversion is necessary
            if not pd.api.types.is_numeric_dtype(df_cleaned[col]):
                initial_nulls = df_cleaned[col].isnull().sum()
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                final_nulls = df_cleaned[col].isnull().sum()
                # If new NaNs were created, increment issue counter
                if final_nulls > initial_nulls:
                     rows_with_numeric_issues += (final_nulls - initial_nulls) # Count how many values failed conversion

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

    # Ensure Quantity and Demand are integer types if possible (after handling NaNs)
    try:
        df_cleaned['Quantity'] = df_cleaned['Quantity'].astype(int)
        df_cleaned['Demand (Required)'] = df_cleaned['Demand (Required)'].astype(int)
    except Exception as e:
        st.warning(f"Could not convert Quantity/Demand to integers: {e}", icon="ℹ️")


    return df_cleaned

def highlight_demand(row):
    """Applies background color based on Quantity vs Demand."""
    # Ensure values are numeric before comparison
    demand = pd.to_numeric(row.get('Demand (Required)'), errors='coerce')
    quantity = pd.to_numeric(row.get('Quantity'), errors='coerce')
    num_cols = len(row)

    # Default style if data is missing/invalid
    default_style = ['background-color: none'] * num_cols # No background for default

    if pd.isna(demand) or pd.isna(quantity):
        return default_style

    # Apply styles based on comparison
    if demand > quantity:
        # Red background for shortage (from legend-red)
        return ['background-color: #fee2e2'] * num_cols
    elif demand == quantity:
        # Orange/Yellow background for exact match (from legend-orange)
        return ['background-color: #ffedd5'] * num_cols
    else: # demand < quantity
        # Green background for surplus (from legend-green)
        return ['background-color: #dcfce7'] * num_cols

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
with st.spinner("Loading Excel data..."):
    df_inventory_raw, df_orders_raw = load_all_excel_data()
with st.spinner("Loading historical demand data..."):
    df_history_demand = load_historical_demand_data()
with st.spinner("Loading forecast data from BigQuery..."):
    df_forecast_demand = load_bigquery_forecast(bq_client) # Pass client

# --- Process Data ---
with st.spinner("Processing inventory data..."):
    df_inventory_cleaned = clean_and_validate_inventory(df_inventory_raw)
# Simple assignment for orders for now, add cleaning if needed later
df_orders_display = df_orders_raw

# --- Main Header Option 3 ---
st.markdown("""
<style>
.custom-header-container {
    background-color: #f0f5f9; /* Light blue-grey background */
    padding: 1.5rem 2rem;
    border-radius: 10px;
    border-left: 6px solid #1a73e8; /* Accent color border */
    margin-bottom: 2rem;
    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}
.custom-header-container h1 {
    color: #0d47a1; /* Darker blue */
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
    color: #333; /* Dark grey text */
    font-size: 1.1rem;
    justify-content: center;
    margin-bottom: 0;
}
</style>
""", unsafe_allow_html=True)

# You'll need an icon image file (e.g., icon.png) in the same directory or specify a path
# Or use an online SVG URL
icon_url = "https://cdn3.iconfinder.com/data/icons/supply-chain-dazzle-series/256/Supply_Chain_Management_SCM-1024.png" # Example icon URL

st.markdown(f"""
<div class="custom-header-container">
    <h1><img src="{icon_url}" alt="Logistics Icon"> Supply Chain Intelligence Hub</h1>
    <p>Drive efficiency with integrated Sales Forecasting, Inventory Management, Order Tracking, and Route Optimization.</p>
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
# --- Render Demand Forecast Tab ---
with tab_demand:
    # Wrap content in a div for potential scoping (optional but good practice)
    st.markdown('<div class="forecast-tab-content">', unsafe_allow_html=True)

    # Main Tab Header (already styled by .tab-header CSS)
    st.markdown('<h2 class="tab-header">Supply & Demand Overview</h2>', unsafe_allow_html=True)

    # --- Historical Demand Section (from CSV) ---
    st.markdown('<h3 class="sub-header">Historical Demand</h3>', unsafe_allow_html=True) # Uses updated .sub-header style
    if df_history_demand is not None:
        if not df_history_demand.empty:
            # Display dataframe (already styled by .stDataFrame CSS)
            st.dataframe(df_history_demand, use_container_width=True)
            st.caption(f"Data loaded from: `{os.path.basename(HISTORY_CSV_PATH)}`")
        else:
            st.info(f"Historical demand file (`{os.path.basename(HISTORY_CSV_PATH)}`) was loaded but is empty.", icon="📄")
    else:
        st.warning(f"Could not load or process historical demand from CSV.", icon="⚠️")
        st.caption(f"Expected file path: `{HISTORY_CSV_PATH}`")

    # --- Forecasted Demand Section (from BigQuery) ---
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Visual separator

    # No need for the extra forecast-section div if we use the divider and sub-header margins
    st.markdown('<h3 class="sub-header">Demand Forecast (BigQuery)</h3>', unsafe_allow_html=True) # Uses updated .sub-header style
    if bq_client is None:
         st.warning("BigQuery connection unavailable, cannot load forecast data.", icon="☁️")
    elif df_forecast_demand is not None:
        if not df_forecast_demand.empty:
             # Display dataframe (already styled by .stDataFrame CSS)
            st.dataframe(df_forecast_demand, use_container_width=True)
            st.caption(f"Data loaded from BigQuery table: `{BQ_FORECAST_TABLE_ID}`")
        else:
            st.info(f"Forecast data table (`{BQ_FORECAST_TABLE_ID}`) is empty or returned no results.", icon="📄")
    else:
         st.warning("Could not load forecast data from BigQuery.", icon="☁️")
         st.caption(f"Target table: `{BQ_FORECAST_TABLE_ID}`")

    # Close the wrapping div
    st.markdown('</div>', unsafe_allow_html=True)

# --- Render Inventory Tab ---
with tab_inventory:
    st.markdown('<h2 class="tab-header">Inventory Status</h2>', unsafe_allow_html=True)

    if df_inventory_cleaned is not None and not df_inventory_cleaned.empty:
        # Calculate metrics AFTER cleaning
        total_items = len(df_inventory_cleaned)
        # Ensure cols are numeric before comparison (should be handled by cleaning, but double-check)
        df_inventory_cleaned['Demand (Required)'] = pd.to_numeric(df_inventory_cleaned['Demand (Required)'], errors='coerce')
        df_inventory_cleaned['Quantity'] = pd.to_numeric(df_inventory_cleaned['Quantity'], errors='coerce')
        valid_comparison_data = df_inventory_cleaned.dropna(subset=['Demand (Required)', 'Quantity'])

        shortages = len(valid_comparison_data[valid_comparison_data['Demand (Required)'] > valid_comparison_data['Quantity']])
        exact_match = len(valid_comparison_data[valid_comparison_data['Demand (Required)'] == valid_comparison_data['Quantity']])
        surplus = len(valid_comparison_data[valid_comparison_data['Demand (Required)'] < valid_comparison_data['Quantity']])

        # --- Display Cards Horizontally using st.columns and custom CSS ---
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        # Use columns to structure, but rely on the CSS for flex layout within the container
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

        st.markdown("<br>", unsafe_allow_html=True) # Add some space before legend

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
        st.dataframe(
            df_inventory_cleaned.style.apply(highlight_demand, axis=1),
            use_container_width=True
        )
        st.caption(f"Data loaded and processed from: `{os.path.basename(INVENTORY_EXCEL_PATH)}`")

    elif df_inventory_raw is not None: # If raw data exists but cleaning failed
         st.warning("Inventory data loaded but could not be processed or is empty after cleaning.", icon="⚠️")
         st.caption(f"Source file: `{os.path.basename(INVENTORY_EXCEL_PATH)}`")
    else: # If raw data failed to load
        st.info("Inventory data unavailable.", icon="ℹ️")
        st.caption(f"Expected file: `{INVENTORY_EXCEL_PATH}`")


# --- Render Orders Tab ---
with tab_orders:
    st.markdown('<h2 class="tab-header">Order Management</h2>', unsafe_allow_html=True)

    if df_orders_display is not None and not df_orders_display.empty:
        total_orders = len(df_orders_display)
        # Add more metrics if available in df_orders_display
        # Example: total_order_value = df_orders_display['Total Value'].sum()

        # --- Display Cards ---
        # Using a container even for one card for consistency
        st.markdown('<div class="card-container">', unsafe_allow_html=True)
        # Use just one column or let flexbox handle a single item
        st.markdown(f"""
            <div class="info-card" style="flex-grow: 0.5;"> <!-- Adjust flex-grow if needed -->
                <span class="card-label">Total Orders Loaded</span>
                <span class="card-value">{total_orders}</span>
            </div>
            """, unsafe_allow_html=True)
        # Add more cards here if needed, e.g.:
        # st.markdown(f'''
        # <div class="success-card" style="flex-grow: 0.5;">
        #     <span class="card-label">Total Order Value</span>
        #     <span class="card-value">${total_order_value:,.2f}</span>
        # </div>
        # ''', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        st.dataframe(df_orders_display, use_container_width=True)
        st.caption(f"Data loaded from: `{os.path.basename(ORDER_EXCEL_PATH)}`")

    elif df_orders_display is not None: # Loaded but empty
        st.info(f"Order Management file (`{os.path.basename(ORDER_EXCEL_PATH)}`) loaded but is empty.", icon="📄")
    else: # Failed to load
        st.info("Order Management data could not be loaded.", icon="ℹ️")
        st.caption(f"Expected file: `{ORDER_EXCEL_PATH}`")

# --- Render Rider Route Tab ---
with tab_route:
    st.markdown('<h2 class="tab-header">Rider Route Visualization</h2>', unsafe_allow_html=True)

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
                        # Merge route sequence with location coordinates
                        route_details_df = pd.merge(
                            rider_route_df.sort_values(by='Seq'), # Ensure sorted by sequence
                            locations_df,
                            on='LocID',
                            how='left'
                        )

                        # Check for missing coordinates after merge
                        missing_coords = route_details_df['Lat'].isnull().sum() + route_details_df['Long'].isnull().sum()
                        if missing_coords > 0:
                             st.warning(f"{missing_coords // 2} locations in the route are missing coordinates and will be excluded from the map.", icon="⚠️")
                             route_details_df.dropna(subset=['Lat', 'Long'], inplace=True)

                        if route_details_df.empty:
                             st.warning("No locations with valid coordinates found for this route.", icon="🙁")
                        else:
                            # --- Fetch Actual Road Route from OSRM ---
                            with st.spinner("Fetching road directions from OSRM..."):
                                actual_route_path = get_osrm_route(route_details_df[['Long', 'Lat']]) # Pass only coords

                            # Prepare path data for PyDeck
                            path_layer_data = None
                            path_color = [255, 165, 0, 180] # Default: Orange for straight line fallback

                            if actual_route_path:
                                path_layer_data = pd.DataFrame({'path': [actual_route_path]})
                                path_color = [0, 128, 255, 200] # Blue for actual road route
                            elif route_details_df.shape[0] >= 2 : # Fallback to straight lines if OSRM failed but >1 point
                                st.info("Could not fetch road directions. Drawing straight lines between stops.")
                                straight_line_path = route_details_df[['Long', 'Lat']].values.tolist()
                                path_layer_data = pd.DataFrame({'path': [straight_line_path]})
                                # Keep path_color as orange
                            else:
                                st.info("Only one valid point, cannot draw a path.")


                            # --- Define Icon Data ---
                            def get_icon_data(loc_id, seq, max_seq):
                                base_scale = 1.0 # Base size scale
                                # Check if it's the DC (using constant)
                                is_dc = (str(loc_id) == DC_LOC_ID)
                                # Check if it's the first or last stop (potentially the same DC)
                                is_start = (seq == route_details_df['Seq'].min())
                                is_end = (seq == max_seq) # Use max_seq passed to function

                                icon_url = DC_PIN_URL if is_dc else STORE_PIN_URL
                                # Make start/end DC slightly larger? Or just color differently?
                                size_multiplier = 1.5 if is_dc and (is_start or is_end) else (1.2 if is_dc else 1.0)

                                return {
                                    "url": icon_url,
                                    "width": int(PIN_WIDTH * size_multiplier * base_scale) ,
                                    "height": int(PIN_HEIGHT * size_multiplier * base_scale),
                                    "anchorY": int(PIN_HEIGHT * size_multiplier * base_scale * PIN_ANCHOR_Y_FACTOR),
                                    "size": int(40 * size_multiplier) # Control the clickable size area
                                    }

                            max_sequence = route_details_df['Seq'].max() # Calculate max sequence once
                            route_details_df['icon_data'] = route_details_df.apply(
                                lambda row: get_icon_data(row['LocID'], row['Seq'], max_sequence), axis=1
                            )

                            # --- PyDeck Rendering ---
                            try:
                                initial_latitude = route_details_df['Lat'].mean()
                                initial_longitude = route_details_df['Long'].mean()
                                initial_view_state = pdk.ViewState(
                                    latitude=initial_latitude,
                                    longitude=initial_longitude,
                                    zoom=11, # Adjust zoom as needed
                                    pitch=45, # Angled view
                                    bearing=0
                                )
                            except Exception: # Fallback if mean fails (e.g., single point)
                                initial_view_state = pdk.ViewState(
                                    latitude=35.1495, longitude=-90.0490, zoom=10, pitch=30 # Memphis fallback
                                )

                            layers = []
                            # Path Layer (only if data exists)
                            if path_layer_data is not None:
                                path_layer = pdk.Layer(
                                    "PathLayer",
                                    data=path_layer_data,
                                    get_path="path",
                                    get_color=path_color,
                                    width_min_pixels=5, # Thicker line
                                    pickable=False # Path itself isn't usually interactive
                                )
                                layers.append(path_layer)

                            # Icon Layer
                            icon_layer = pdk.Layer(
                                "IconLayer",
                                data=route_details_df,
                                get_icon="icon_data", # Fetches dict from the column
                                get_position=["Long", "Lat"],
                                get_size='icon_data.height', # Use height for scaling control
                                size_scale=1, # Use direct pixel values from icon_data
                                pickable=True,
                                auto_highlight=True,
                                highlight_color=[255, 255, 0, 150] # Yellow highlight
                             )
                            layers.append(icon_layer)

                            # Tooltip (Improved formatting)
                            tooltip = {
                                "html": """
                                <div style='background-color: rgba(0,0,0,0.7); color: white; padding: 8px 12px; border-radius: 5px; font-family: sans-serif; font-size: 0.9em;'>
                                    <b>{LocName}</b><br/>
                                    Stop #: {Seq}<br/>
                                    ID: {LocID}<br/>
                                    Coords: {Lat:.4f}, {Long:.4f}
                                </div>
                                """,
                                "style": { # This style block in tooltip often doesn't work well, use CSS class instead if needed
                                     "backgroundColor": "rgba(0,0,0,0)", # Try overriding default tooltip style box if CSS fails
                                     "color": "white"
                                 }
                            }


                            # Render the map
                            st.pydeck_chart(pdk.Deck(
                                map_style="mapbox://styles/mapbox/light-v10", # Light style
                                # map_style="mapbox://styles/mapbox/satellite-streets-v11", # Alt: Satellite
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
                            first_seq = route_details_df.iloc[0]['Seq'] if not route_details_df.empty else -1
                            last_seq = route_details_df.iloc[-1]['Seq'] if not route_details_df.empty else -1


                            for index, row in route_details_df.iterrows():
                                loc_name = row['LocName']
                                loc_id = row['LocID']
                                seq = row['Seq']
                                prefix = ""
                                icon = "📍" # Default stop icon

                                if seq == first_seq and str(loc_id) == DC_LOC_ID:
                                    prefix = f"**Start (DC):** "
                                    icon = "🏭"
                                elif seq == last_seq and str(loc_id) == DC_LOC_ID and seq != first_seq:
                                     prefix = f"**End (Return DC):** "
                                     icon = "🏭"
                                elif seq == first_seq: # Start but not DC
                                    prefix = f"**Start (Stop 1):** "
                                    icon = "🏁"
                                elif seq == last_seq: # End but not DC
                                     prefix = f"**End (Stop {seq - (1 if str(first_loc_id)==DC_LOC_ID else 0)}):** "
                                     icon = "🏁"
                                else: # Intermediate stop
                                    # Adjust stop number if started at DC
                                    stop_num = seq - 1 if str(first_loc_id) == DC_LOC_ID else seq
                                    prefix = f"**Stop {stop_num}:** "

                                summary_items.append(f"* {icon} {prefix} {loc_name} (`{loc_id}`)")

                            st.markdown("\n".join(summary_items))


                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

                            # --- Route Details Table ---
                            st.markdown("#### Route Stop Details")
                            st.dataframe(
                                route_details_df[['Seq', 'LocID', 'LocName', 'Lat', 'Long']].reset_index(drop=True),
                                use_container_width=True,
                                hide_index=True # Cleaner look
                                )

        elif selected_week is None or selected_rider is None:
            st.info("Select a Week and Rider above to view the route details and map.", icon="👆")


# --- Footer ---
st.markdown('<p class="footer-caption">Business Operations Dashboard | Enhanced UI Demo</p>', unsafe_allow_html=True)
