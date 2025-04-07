# 1. Consolidated Imports
# =============================================================================
import streamlit as st
import pandas as pd
import pydeck as pdk
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import numpy as np
import sys
import requests
import polyline # For decoding OSRM polylines
import traceback
import base64
import asyncio
import logging
import re
import json
import urllib.request
import urllib.parse
import random
import datetime
from typing import List, Dict, Any, Optional, Union

# OR-Tools Imports
try:
    from ortools.constraint_solver import routing_enums_pb2
    from ortools.constraint_solver import pywrapcp
    ORTOOLS_AVAILABLE = True
except ImportError:
    st.error("Error: `ortools` library not found. Chatbot route generation will fail. Install it (`pip install ortools`).", icon="⚠️")
    ORTOOLS_AVAILABLE = False

# Google Cloud / Vertex AI / Agno Imports
try:
    from google.auth import default, exceptions as google_auth_exceptions
    from google.api_core.exceptions import GoogleAPIError, NotFound, Forbidden
    # Make sure 'agno' is installed and imported correctly
    from agno.models.google import Gemini as AgnoGemini
    from agno.agent import Agent
    from agno.team import Team
    from agno.exceptions import ModelProviderError
    from vertexai.generative_models import Content, FunctionDeclaration, GenerativeModel, Part, Tool
    import vertexai.preview.generative_models as generative_models # Using preview
    from google.cloud import aiplatform
    GOOGLE_CLOUD_AVAILABLE = True
except ImportError as e:
    st.error(f"Error: Missing Google Cloud or Agno library: {e}. Chatbot features will fail. Install required packages.", icon="☁️")
    GOOGLE_CLOUD_AVAILABLE = False
    # Define dummy classes/functions if needed to prevent NameErrors later, though checks should prevent usage
    class DummyModel: pass
    class Agent: pass
    class Team: pass
    AgnoGemini = DummyModel
    GenerativeModel = DummyModel


# Try importing optional dashboard libraries
try:
    import openpyxl
    import db_dtypes # Required for BigQuery nullable integers/floats
    DASHBOARD_LIBS_AVAILABLE = True
except ImportError as e:
    missing_lib = str(e).split("'")[-2]
    st.warning(f"Warning: Missing optional library '{missing_lib}'. Dashboard features might be affected. Install (`pip install {missing_lib}`).", icon="📦")
    DASHBOARD_LIBS_AVAILABLE = False # Track availability


# =============================================================================
# 2. Page Configuration (Single App)
# =============================================================================
# MUST BE THE VERY FIRST STREAMLIT COMMAND
st.set_page_config(
    page_title="Integrated Supply Chain Hub",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# 3. Consolidated Constants
# =============================================================================
# --- GCP/BigQuery Config ---
PROJECT_ID = "gebu-data-ml-day0-01-333910" # <-- REPLACE if needed
LOCATION = "us-central1" # For Vertex AI
GCP_PROJECT_ID = PROJECT_ID # Alias used in original dashboard code

BQ_DATASET = "supply_chain"
BQ_DATASET_ID = BQ_DATASET # Alias used in original chatbot code

BQ_FORECAST_DATASET = "demand_forecast"
BQ_FORECAST_TABLE_ID = f"{PROJECT_ID}.{BQ_FORECAST_DATASET}.forecast1" # <-- CHECK/UPDATE this table name
BQ_LOCATIONS_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.locations"
BQ_ROUTES_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.routes" # Used by both dashboard and chatbot BQ interactions
BQ_PRODUCTS_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.product_inventory" # Used by dashboard inventory and chatbot replenishment

# --- Chatbot Specific Table Names/IDs ---
REPLENISH_TABLE_NAME = "product_inventory" # Name within BQ_DATASET
REPLENISH_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{REPLENISH_TABLE_NAME}" # Full ID

# --- Dashboard File Paths ---
# WARNING: Absolute paths limit portability. Use relative paths or environment variables if possible.
ORDER_EXCEL_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\Order Management.xlsx"
HISTORY_CSV_PATH = r"C:\Users\revanthvenkat.bhuva\Desktop\browser_use\supply_chain_management\Historical Product Demand.csv"

# --- Dashboard PyDeck Config ---
DC_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-red.png"
STORE_PIN_URL = "https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-2x-blue.png"
PIN_WIDTH = 25
PIN_HEIGHT = 41
PIN_ANCHOR_Y_FACTOR = 1.0
DC_LOC_ID = 'LOC0' # Assuming 'LOC0' is always the Distribution Center ID

# --- Chatbot Config ---
GOOGLE_MAPS_API_KEY = "AIzaSyDJjAkgtwN0weYaoKFud_Xn3h5YDNG1q14" # <-- REPLACE WITH YOUR KEY
MODEL_ID = "gemini-2.0-flash" # Default model
ROUTE_AGENT_MODEL_ID = "gemini-2.0-flash"
NL_BQ_AGENT_MODEL_ID = "gemini-2.0-flash"
REPLENISH_AGENT_MODEL_ID = "gemini-2.0-flash"
TEAM_ROUTER_MODEL_ID = "gemini-2.0-flash"

BASE_ADDRESSES = [ # For Chatbot OR-Tools
    '3610+Hacks+Cross+Rd+Memphis+TN', # 0
    '1921+Elvis+Presley+Blvd+Memphis+TN', # 1
    '149+Union+Avenue+Memphis+TN', # 2
    '1034+Audubon+Drive+Memphis+TN', # 3
    '1532+Madison+Ave+Memphis+TN', # 4
    '706+Union+Ave+Memphis+TN', # 5
    '3641+Central+Ave+Memphis+TN', # 6
    '926+E+McLemore+Ave+Memphis+TN', # 7
    '4339+Park+Ave+Memphis+TN', # 8
    '600+Goodwyn+St+Memphis+TN', # 9
    '2000+North+Pkwy+Memphis+TN', # 10
    '262+Danny+Thomas+Pl+Memphis+TN', # 11
    '125+N+Front+St+Memphis+TN', # 12
    '5959+Park+Ave+Memphis+TN', # 13
    '814+Scott+St+Memphis+TN', # 14
    '1005+Tillman+St+Memphis+TN' # 15
]

# =============================================================================
# 4. Logging Setup
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IntegratedApp") # Use a specific logger name

# =============================================================================
# 5. Shared Resource Initialization (BQ Client, Vertex AI)
# =============================================================================

# --- BigQuery Client Initialization ---
@st.cache_resource
def initialize_bq_client():
    """Initializes and returns a BigQuery client using various methods."""
    client = None
    credentials = None
    auth_method = "None"
    logger.info("Attempting to initialize BigQuery client...")

    # Check for essential Project ID
    if not PROJECT_ID or "your-gcp-project" in PROJECT_ID:
        logger.error("GCP Project ID is not set correctly in constants.")
        st.error("Configuration Error: GCP Project ID is missing or invalid.", icon="🚨")
        return None

    # 1. Try Streamlit Secrets (Preferred for deployed apps)
    try:
        # Check if secrets are loaded and key exists
        if hasattr(st, 'secrets') and 'gcp_service_account' in st.secrets:
            logger.info("Attempting connection via Streamlit Secrets...")
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["gcp_service_account"]
            )
            client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
            client.list_datasets(max_results=1) # Test query lighter than SELECT 1
            auth_method = "Streamlit Secrets"
            logger.info(f"BigQuery Connection Successful ({auth_method}).")
            return client
        else:
            logger.info("Streamlit Secrets not found or 'gcp_service_account' key missing.")
    except Exception as e:
        logger.warning(f"Connection via Streamlit Secrets failed: {e}")

    # 2. Try Application Default Credentials (ADC) (Good for local dev/cloud envs)
    try:
        if not client:
            logger.info("Attempting connection via Application Default Credentials (ADC)...")
            # location="US" might be needed depending on your BQ setup
            client = bigquery.Client(project=PROJECT_ID)
            client.list_datasets(max_results=1) # Test query
            auth_method = "Application Default Credentials (ADC)"
            logger.info(f"BigQuery Connection Successful ({auth_method}).")
            return client
    except google_auth_exceptions.DefaultCredentialsError:
         logger.warning("ADC not found or not configured correctly.")
    except Exception as e:
        logger.warning(f"Connection via ADC failed: {e}")

    # 3. Try Environment Variable (GOOGLE_APPLICATION_CREDENTIALS)
    try:
        if not client:
            credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if credentials_path:
                 logger.info(f"Attempting connection via GOOGLE_APPLICATION_CREDENTIALS: {credentials_path}")
                 if os.path.exists(credentials_path):
                     credentials = service_account.Credentials.from_service_account_file(credentials_path)
                     client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
                     client.list_datasets(max_results=1) # Test query
                     auth_method = "GOOGLE_APPLICATION_CREDENTIALS Env Var"
                     logger.info(f"BigQuery Connection Successful ({auth_method}).")
                     return client
                 else:
                      logger.warning(f"GOOGLE_APPLICATION_CREDENTIALS path specified but file not found: {credentials_path}")
            else:
                 logger.info("GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
    except Exception as e:
        logger.warning(f"Connection via GOOGLE_APPLICATION_CREDENTIALS failed: {e}")

    # If all methods fail
    logger.error("Fatal: Could not connect to BigQuery using any available method.")
    # Don't use st.error here directly, let the main app logic handle it
    return None # Return None if connection failed

# Initialize the client globally for the app session
bq_client = initialize_bq_client()

# --- Vertex AI Initialization (for Chatbot) ---
VERTEX_AI_INITIALIZED = False
# Only attempt if BQ client succeeded (as some chatbot tools might need BQ indirectly)
# and if Google Cloud libraries were imported successfully
if bq_client and GOOGLE_CLOUD_AVAILABLE:
    # Check for essential Chatbot API Key needed for some tools
    if not GOOGLE_MAPS_API_KEY or "YOUR_GOOGLE_MAPS_API_KEY" in GOOGLE_MAPS_API_KEY:
        logger.warning("Google Maps API Key missing or invalid. Chatbot route generation tool will fail.")
        # Don't stop Vertex AI init, just warn

    try:
        logger.info("Attempting to initialize Vertex AI...")
        aiplatform.init(project=PROJECT_ID, location=LOCATION)
        # Optionally test the connection, e.g., by listing models, but can add latency
        # aiplatform.Model.list(location=LOCATION)
        logger.info(f"Vertex AI Initialized. Project: {PROJECT_ID}, Location: {LOCATION}")
        VERTEX_AI_INITIALIZED = True
    except Exception as e:
        logger.error(f"Failed to initialize Vertex AI SDK: {e}", exc_info=True)
        # Inform user, but don't stop the whole app if dashboard might work
        st.warning(f"Vertex AI Initialization Error: {e}. Chatbot features may be unavailable.", icon="🤖")
        VERTEX_AI_INITIALIZED = False
elif not GOOGLE_CLOUD_AVAILABLE:
    logger.error("Skipping Vertex AI initialization because required Google Cloud libraries are missing.")
    # Error already shown during import
else: # bq_client is None
    logger.warning("Skipping Vertex AI initialization because BigQuery client failed.")
    # Error for BQ failure shown elsewhere

# =============================================================================
# 6. DASHBOARD SPECIFIC CODE (Functions and Styling)
# =============================================================================

# --- Dashboard Styling ---
DASHBOARD_APP_STYLE = """
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
        /* background-color: #f8f9fa; */ /* Removed background */
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

    /* Specific Chatbot Styles (Can be added here or applied conditionally) */
    /* Example for chat input styling: */
    /* div[data-testid="stChatInput"] textarea {
        background-color: #e3f2fd;
        border: 1px solid #1a73e8;
    } */

</style>
"""

def apply_dashboard_styling():
    """Applies the dashboard-specific CSS."""
    st.markdown(DASHBOARD_APP_STYLE, unsafe_allow_html=True)

# --- Dashboard Data Loading & Processing Functions ---

def load_excel(file_path, data_label="Data"):
    """Loads data from an Excel file (Dashboard)."""
    logger.info(f"Dashboard: Loading Excel: {file_path}")
    if not os.path.exists(file_path):
        st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
        return None
    try:
        # Make sure openpyxl is installed
        if not DASHBOARD_LIBS_AVAILABLE or 'openpyxl' not in sys.modules:
             st.error("`openpyxl` library not found or failed to import. Cannot read Excel file.", icon="📦")
             return None
        df = pd.read_excel(file_path, engine='openpyxl')
        df.columns = df.columns.str.strip()
        if df.empty: st.warning(f"{data_label} Warning: File is empty: `{os.path.basename(file_path)}`", icon="⚠️")
        return df
    except FileNotFoundError: # Should be caught by os.path.exists, but belt-and-suspenders
        st.error(f"{data_label} Error: File not found at `{file_path}`", icon="❌")
        return None
    except Exception as e:
        st.error(f"An error occurred reading {data_label} file ({os.path.basename(file_path)}): {e}", icon="❌")
        traceback.print_exc()
        return None

def load_csv(file_path, data_label="Data"):
    """Loads data from a CSV file (Dashboard)."""
    logger.info(f"Dashboard: Loading CSV: {file_path}")
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
        st.error(f"An error occurred reading {data_label} CSV file ({os.path.basename(file_path)}): {e}", icon="❌")
        traceback.print_exc()
        return None

@st.cache_data
def load_historical_demand_data():
    """Loads historical demand data from CSV (Dashboard)."""
    return load_csv(HISTORY_CSV_PATH, "Historical Demand")

@st.cache_data(ttl=1800) # Cache inventory data for 30 mins
def load_bigquery_inventory(_client):
    """Loads inventory data from BigQuery (Dashboard)."""
    logger.info("Dashboard: Loading BigQuery Inventory")
    if not _client:
        # This check is redundant if called after global check, but safe
        st.error("BigQuery client not available. Cannot load inventory data.", icon="☁️")
        return None
    # Ensure db-dtypes is available for optimal BQ type handling
    if not DASHBOARD_LIBS_AVAILABLE or 'db_dtypes' not in sys.modules:
         st.error("`db-dtypes` library not available. Cannot reliably handle BigQuery nullable types.", icon="📦")
         # Fallback: Load without specific dtypes (might cause issues later)
         try:
             query = f"SELECT * FROM `{BQ_PRODUCTS_TABLE_ID}`"
             df = _client.query(query).to_dataframe(create_bqstorage_client=True)
             logger.warning("Loaded BQ Inventory without db-dtypes. Type issues may occur.")
             # Continue with renaming, but subsequent cleaning needs to be robust
         except Exception as e:
            st.error(f"Error loading inventory data (fallback) from BigQuery: {e}", icon="☁️")
            traceback.print_exc()
            return None
    else:
        # Load using db-dtypes
        query = f"SELECT * FROM `{BQ_PRODUCTS_TABLE_ID}`"
        try:
            df = _client.query(query).to_dataframe(
                create_bqstorage_client=True,
                dtypes={ # Specify expected types using db-dtypes
                    "Price__USD_": pd.Float64Dtype(),
                    "Quantity": pd.Int64Dtype(),
                    "Discount____": pd.Float64Dtype(),
                    "Demand__Required_": pd.Int64Dtype()
                    # Add other columns and their BQ types if needed
                }
            )
            logger.debug(f"BQ Inventory Raw Columns: {df.columns.tolist()}")
        except Exception as e:
            st.error(f"Error loading inventory data using db-dtypes from BigQuery: {e}", icon="☁️")
            traceback.print_exc()
            return None

    # Proceed with renaming regardless of how it was loaded (best effort)
    if df is None: return None

    # --- Rename columns ---
    column_mapping = {
        'Product_ID': 'Product ID',
        'Product_Name': 'Product Name',
        'Price__USD_': 'Price (USD)',
        'Description': 'Description',
        'Quantity': 'Quantity',
        'Discount____': 'Discount (%)',
        'Country_of_Origin': 'Country of Origin',
        'Demand__Required_': 'Demand (Required)'
    }
    rename_map = {bq_col: app_col for bq_col, app_col in column_mapping.items() if bq_col in df.columns}
    missing_expected = [app_col for bq_col, app_col in column_mapping.items() if bq_col not in df.columns]
    if missing_expected:
         logger.warning(f"BQ Inventory table missing expected columns for mapping: {missing_expected}")

    try:
        # Select only the columns we can map and rename them
        df_renamed = df[list(rename_map.keys())].rename(columns=rename_map)
        logger.debug(f"BQ Inventory Renamed Columns: {df_renamed.columns.tolist()}")
        return df_renamed
    except KeyError as ke:
         st.error(f"Error during inventory column renaming: Column {ke} not found in loaded data. Check BQ table structure.")
         return None
    except Exception as e_rename:
         st.error(f"Unexpected error during inventory column renaming: {e_rename}")
         return None


@st.cache_data(ttl=600)
def get_available_weeks_riders(_client):
    """Gets unique WeekNo and RiderID combinations from routes table (Dashboard)."""
    logger.info("Dashboard: Getting available weeks/riders")
    if not _client: return pd.DataFrame({'WeekNo': [], 'RiderID': []})

    query = f"SELECT DISTINCT WeekNo, RiderID FROM `{BQ_ROUTES_TABLE_ID}` ORDER BY WeekNo DESC, RiderID ASC"
    try:
        # Use db-dtypes if available for nullable types
        dtype_param = {"WeekNo": pd.Int64Dtype()} if ('db_dtypes' in sys.modules and DASHBOARD_LIBS_AVAILABLE) else None
        df = _client.query(query).to_dataframe(
            create_bqstorage_client=True,
            dtypes=dtype_param
        )
        # Ensure WeekNo is numeric-like if db-dtypes wasn't used
        if 'WeekNo' in df.columns and dtype_param is None:
             df['WeekNo'] = pd.to_numeric(df['WeekNo'], errors='coerce')
             df.dropna(subset=['WeekNo'], inplace=True) # Drop rows where conversion failed
             df['WeekNo'] = df['WeekNo'].astype(int) # Convert to standard int if possible after dropping NA

        return df
    except Exception as e:
        st.error(f"Error fetching week/rider data from BigQuery: {e}", icon="☁️")
        traceback.print_exc()
        return pd.DataFrame({'WeekNo': pd.Series(dtype='Int64'), 'RiderID': pd.Series(dtype='str')})

@st.cache_data(ttl=600)
def get_route_data(_client, week: Optional[int], rider: Optional[str]):
    """Fetches route sequence for a specific week and rider (Dashboard)."""
    logger.info(f"Dashboard: Getting route data W{week} R{rider}")
    if not _client: return pd.DataFrame({'Seq': [], 'LocID': []})
    if week is None or rider is None:
         # Don't show error, just return empty if selection not made
         # st.warning("Week or Rider not selected.")
         return pd.DataFrame({'Seq': [], 'LocID': []})

    try:
        week_int = int(week)
    except (ValueError, TypeError):
        st.error(f"Invalid week number provided: {week}", icon="❌")
        return pd.DataFrame({'Seq': [], 'LocID': []})

    query = f"""
        SELECT Seq, LocID
        FROM `{BQ_ROUTES_TABLE_ID}`
        WHERE WeekNo = @week_no AND RiderID = @rider_id
        ORDER BY CAST(Seq AS BIGNUMERIC) ASC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("week_no", "INT64", week_int),
        bigquery.ScalarQueryParameter("rider_id", "STRING", rider)
    ])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        if 'Seq' in df.columns:
            # Convert Seq robustly to nullable integer
            df['Seq'] = pd.to_numeric(df['Seq'], errors='coerce')
            df.dropna(subset=['Seq'], inplace=True) # Remove rows where Seq couldn't be converted
            # Use nullable int type if available and needed, otherwise float might be safer if NAs existed
            if 'db_dtypes' in sys.modules and DASHBOARD_LIBS_AVAILABLE:
                df['Seq'] = df['Seq'].astype(pd.Int64Dtype())
            else:
                 # Fallback: if no NAs remain, use int, otherwise keep float
                 if not df['Seq'].isnull().any():
                      df['Seq'] = df['Seq'].astype(int)
        else:
             logger.warning("Route data fetched but 'Seq' column is missing.")
             return pd.DataFrame({'Seq': [], 'LocID': []})
        return df
    except Exception as e:
        st.error(f"Error fetching route data for W{week}, R{rider}: {e}", icon="☁️")
        traceback.print_exc()
        return pd.DataFrame({'Seq': [], 'LocID': []})

@st.cache_data(ttl=3600)
def get_location_data(_client, loc_ids: list):
    """Fetches location details (Lat, Long, Name) for given LocIDs (Dashboard)."""
    logger.info(f"Dashboard: Getting location data for {len(loc_ids)} IDs")
    if not _client: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})
    if not loc_ids: return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

    valid_loc_ids = [str(loc) for loc in loc_ids if pd.notna(loc) and str(loc).strip()]
    if not valid_loc_ids:
        logger.warning("No valid LocIDs provided to get_location_data after filtering.")
        return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

    query = f"""
        SELECT LocID, LocName, Lat, Long
        FROM `{BQ_LOCATIONS_TABLE}`
        WHERE CAST(LocID AS STRING) IN UNNEST(@loc_ids)
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("loc_ids", "STRING", valid_loc_ids)
    ])
    try:
        df = _client.query(query, job_config=job_config).to_dataframe(create_bqstorage_client=True)
        df['Lat'] = pd.to_numeric(df['Lat'], errors='coerce')
        df['Long'] = pd.to_numeric(df['Long'], errors='coerce')
        df['LocID'] = df['LocID'].astype(str) # Ensure LocID is string
        return df
    except Exception as e:
        st.error(f"Error fetching location data: {e}", icon="☁️")
        traceback.print_exc()
        return pd.DataFrame({'LocID': [], 'LocName': [], 'Lat': [], 'Long': []})

@st.cache_data(ttl=1800)
def load_bigquery_forecast(_client):
    """Loads forecast data from BigQuery (Dashboard)."""
    logger.info("Dashboard: Loading BigQuery Forecast")
    if not _client: return None

    query = f"SELECT * FROM `{BQ_FORECAST_TABLE_ID}` ORDER BY date DESC"
    try:
        df = _client.query(query).to_dataframe(create_bqstorage_client=True)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df.dropna(subset=['date'], inplace=True) # Date is essential
        else:
            logger.warning(f"Forecast table {BQ_FORECAST_TABLE_ID} missing 'date' column.")
            # Return empty df if date missing? Or allow proceeding? Returning empty for safety.
            return pd.DataFrame()

        numeric_forecast_cols = ['forecast_value', 'actual_value', 'lower_bound', 'upper_bound']
        for col in numeric_forecast_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
    except Exception as e:
        st.error(f"Error loading forecast data from BigQuery table `{BQ_FORECAST_TABLE_ID}`: {e}", icon="☁️")
        traceback.print_exc()
        return None # Indicate failure

def clean_and_validate_inventory(df):
    """Cleans and validates inventory data (Dashboard)."""
    logger.info("Dashboard: Cleaning Inventory Data")
    if df is None:
        logger.warning("Inventory data is None before cleaning.")
        return None
    if df.empty:
        logger.warning("Inventory data is empty before cleaning.")
        return df # Return empty if received empty

    df_cleaned = df.copy()
    required_cols = ['Quantity', 'Demand (Required)', 'Product ID', 'Product Name']
    numeric_cols = ['Price (USD)', 'Quantity', 'Discount (%)', 'Demand (Required)']

    missing_req = [col for col in required_cols if col not in df_cleaned.columns]
    if missing_req:
        st.error(f"Inventory Error: Missing required columns: {', '.join(missing_req)}", icon="❗")
        st.caption(f"Check BQ table `{BQ_PRODUCTS_TABLE_ID}` schema and `load_bigquery_inventory` mapping.")
        return None

    rows_with_numeric_issues = 0
    for col in numeric_cols:
        if col in df_cleaned.columns:
            # Convert if not already numeric (handles object types, etc.)
            if not pd.api.types.is_numeric_dtype(df_cleaned[col]):
                initial_nulls = df_cleaned[col].isnull().sum()
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')
                final_nulls = df_cleaned[col].isnull().sum()
                new_issues = final_nulls - initial_nulls
                if new_issues > 0:
                     rows_with_numeric_issues += new_issues
                     logger.warning(f"Coerced {new_issues} non-numeric values to NaN in column '{col}'")

            # Ensure float type for calculations, handle potential pd.NA from db-dtypes
            if pd.api.types.is_numeric_dtype(df_cleaned[col]): # Check again after potential coercion
                 # Convert Int64Dtype/int to nullable Float64Dtype if NAs might exist or calculations needed
                 if pd.api.types.is_integer_dtype(df_cleaned[col]):
                     try:
                        # Use nullable float if db_dtypes available, otherwise standard float
                        target_float_type = pd.Float64Dtype() if ('db_dtypes' in sys.modules and DASHBOARD_LIBS_AVAILABLE) else 'float64'
                        df_cleaned[col] = df_cleaned[col].astype(target_float_type)
                     except Exception as e_astype:
                          logger.warning(f"Could not convert column '{col}' to float type: {e_astype}")


    if rows_with_numeric_issues > 0:
        st.warning(f"Inventory Warning: {rows_with_numeric_issues} non-numeric values found and ignored (set to NaN).", icon="⚠️")

    initial_rows = len(df_cleaned)
    df_cleaned.dropna(subset=required_cols, inplace=True)
    rows_dropped = initial_rows - len(df_cleaned)
    if rows_dropped > 0:
        st.warning(f"Inventory Warning: {rows_dropped} rows removed due to missing required values ('{', '.join(required_cols)}').", icon="⚠️")

    if df_cleaned.empty:
        st.error("Inventory Error: No valid data remaining after cleaning.", icon="❗")
        return None

    logger.info(f"Inventory cleaning finished. {len(df_cleaned)} rows remaining.")
    return df_cleaned

def highlight_demand(row):
    """Applies background color based on Quantity vs Demand (Dashboard)."""
    demand = pd.to_numeric(row.get('Demand (Required)', np.nan), errors='coerce')
    quantity = pd.to_numeric(row.get('Quantity', np.nan), errors='coerce')
    num_cols = len(row)
    default_style = [''] * num_cols

    if pd.isna(demand) or pd.isna(quantity): return default_style

    try:
        if demand > quantity: return ['background-color: #fee2e2'] * num_cols # Light Red
        elif demand == quantity: return ['background-color: #ffedd5'] * num_cols # Light Orange
        else: return ['background-color: #dcfce7'] * num_cols # Light Green
    except TypeError:
        logger.error(f"Type error comparing demand/quantity in highlight_demand", exc_info=True)
        return default_style

@st.cache_data(ttl=3600)
def get_osrm_route(points_df):
    """Gets road route geometry from OSRM (Dashboard)."""
    logger.info("Dashboard: Fetching OSRM route")
    if points_df is None or points_df.empty or points_df.shape[0] < 2:
        st.warning("Need at least two valid points for OSRM route.", icon="📍")
        return None
    if not all(col in points_df.columns for col in ['Long', 'Lat']):
        st.error("Missing 'Long'/'Lat' columns for OSRM.", icon="❌")
        return None

    valid_points = points_df.dropna(subset=['Long', 'Lat'])
    if valid_points.shape[0] < 2:
        st.warning(f"Not enough valid coordinates ({valid_points.shape[0]}) for OSRM.", icon="📍")
        return None

    locs_str = ";".join([f"{lon},{lat}" for lon, lat in valid_points[['Long', 'Lat']].values])
    osrm_base_url = "http://router.project-osrm.org/route/v1/driving/"
    request_url = f"{osrm_base_url}{locs_str}?overview=full&geometries=polyline"
    logger.debug(f"OSRM Request URL (first 100 chars): {request_url[:100]}...")

    try:
        response = requests.get(request_url, timeout=30)
        response.raise_for_status()
        route_data = response.json()

        if route_data.get('code') == 'Ok' and route_data.get('routes'):
            if not route_data['routes']:
                 st.warning("OSRM returned 'Ok' but no routes found.", icon="✖️")
                 return None
            encoded_polyline = route_data['routes'][0].get('geometry')
            if not encoded_polyline:
                 st.warning("OSRM route found but geometry is missing.", icon="✖️")
                 return None
            # Ensure polyline library is available
            if 'polyline' not in sys.modules:
                 st.error("`polyline` library not found. Cannot decode OSRM route. Install (`pip install polyline`).", icon="📦")
                 return None
            decoded_coords_lat_lon = polyline.decode(encoded_polyline)
            route_path_lon_lat = [[lon, lat] for lat, lon in decoded_coords_lat_lon]
            st.success("Road directions obtained from OSRM.", icon="🗺️")
            return route_path_lon_lat
        else:
            error_msg = route_data.get('message', 'No details provided.')
            logger.warning(f"OSRM API call failed. Code: {route_data.get('code')}, Msg: {error_msg}")
            st.warning(f"OSRM could not find a route: {error_msg}", icon="✖️")
            return None
    except requests.exceptions.Timeout:
        st.error("OSRM API Timeout.", icon="⏱️"); return None
    except requests.exceptions.RequestException as e:
        st.error(f"OSRM API Error: {e}", icon="🌐"); traceback.print_exc(); return None
    except Exception as e:
        st.error(f"OSRM Processing Error: {e}", icon="⚙️"); traceback.print_exc(); return None


# =============================================================================
# 7. CHATBOT SPECIFIC CODE (Functions, Agents, Teams)
# =============================================================================

# --- Chatbot OR-Tools Routing Logic ---

def chatbot_create_distance_matrix(addresses: List[str], api_key: str) -> Optional[List[List[int]]]:
    """Fetches distance matrix using Google Maps API (Chatbot)."""
    logger.info(f"Chatbot: Fetching distance matrix for {len(addresses)} addresses.")
    try:
        max_elements = 100; num_addresses = len(addresses)
        if num_addresses == 0: logger.error("Chatbot: No addresses for distance matrix."); return None
        max_rows = max(1, max_elements // num_addresses if num_addresses > 0 else max_elements)
        q, r = divmod(num_addresses, max_rows); distance_matrix = []
        for i in range(q):
            origin_addresses = addresses[i * max_rows : (i + 1) * max_rows]
            response = chatbot_send_request(origin_addresses, addresses, api_key)
            if response is None: logger.error(f"Chatbot: Send request failed chunk {i}."); return None
            built_matrix = chatbot_build_distance_matrix(response)
            if built_matrix is None: logger.error(f"Chatbot: Build matrix failed chunk {i}."); return None
            distance_matrix.extend(built_matrix)
        if r > 0:
            origin_addresses = addresses[q * max_rows : q * max_rows + r]
            response = chatbot_send_request(origin_addresses, addresses, api_key)
            if response is None: logger.error("Chatbot: Send request failed remainder."); return None
            built_matrix = chatbot_build_distance_matrix(response)
            if built_matrix is None: logger.error("Chatbot: Build matrix failed remainder."); return None
            distance_matrix.extend(built_matrix)
        logger.info("Chatbot: Successfully built distance matrix.")
        if not distance_matrix or len(distance_matrix) != num_addresses or not all(len(row) == num_addresses for row in distance_matrix):
             logger.error(f"Chatbot: Final matrix invalid shape ({len(distance_matrix)}x...).")
             return None
        return distance_matrix
    except Exception as e:
        logger.error(f"Chatbot: Error creating distance matrix: {e}", exc_info=True); return None

def chatbot_send_request(origin_addresses: List[str], dest_addresses: List[str], api_key: str) -> Optional[Dict]:
    """Builds and sends request to the Google Distance Matrix API (Chatbot)."""
    def build_address_str(addr_list): return "|".join(addr_list)
    try:
        url_base = "https://maps.googleapis.com/maps/api/distancematrix/json?"
        units = "imperial"
        origins = urllib.parse.quote(build_address_str(origin_addresses))
        dests = urllib.parse.quote(build_address_str(dest_addresses))
        url = f"{url_base}units={units}&origins={origins}&destinations={dests}&key={api_key}"
        logger.debug(f"Chatbot: Sending Distance Matrix request (URL length: {len(url)})")
        with urllib.request.urlopen(url, timeout=30) as response:
            response_data = json.loads(response.read().decode("utf-8"))
        if response_data.get("status") != "OK":
            error_msg = response_data.get("error_message", "No error message.")
            logger.error(f"Chatbot: Distance Matrix API Error: {response_data.get('status')}. Msg: {error_msg}")
            return None
        return response_data
    except urllib.error.URLError as e:
        logger.error(f"Chatbot: URL Error sending Distance Matrix request: {e}", exc_info=True); return None
    except Exception as e:
        logger.error(f"Chatbot: Error sending Distance Matrix request: {e}", exc_info=True); return None

def chatbot_build_distance_matrix(response: Dict) -> Optional[List[List[int]]]:
    """Parses the API response to build a matrix of distances in meters (Chatbot)."""
    distance_matrix = []
    if not response or 'rows' not in response:
        logger.error("Chatbot: Invalid response format for building distance matrix."); return None
    try:
        expected_cols = len(response.get('destination_addresses', []))
        for i, row in enumerate(response['rows']):
            row_elements = row.get('elements', [])
            if len(row_elements) != expected_cols:
                 logger.error(f"Chatbot: Row {i} inconsistent elements ({len(row_elements)} vs {expected_cols})."); return None
            row_list = []
            for j, element in enumerate(row_elements):
                status = element.get('status')
                if status == "OK":
                    distance_data = element.get('distance')
                    if distance_data and 'value' in distance_data:
                        row_list.append(distance_data['value']) # Meters
                    else:
                        logger.warning(f"Chatbot: Elem ({i},{j}) OK but distance missing. Using large dist."); row_list.append(9999999)
                else:
                    logger.warning(f"Chatbot: Elem ({i},{j}) status '{status}'. Using large dist."); row_list.append(9999999)
            distance_matrix.append(row_list)
        return distance_matrix
    except Exception as e:
        logger.error(f"Chatbot: Error parsing distance matrix response row: {e}", exc_info=True); return None

def chatbot_format_solution(data: Dict, manager: pywrapcp.RoutingIndexManager, routing: pywrapcp.RoutingModel, solution: pywrapcp.Assignment, week_no: int) -> List[Dict[str, Any]]:
    """Formats the OR-Tools solution into a list of dictionaries (Chatbot)."""
    routes_data = []
    logger.info("Chatbot: Formatting OR-Tools solution.")
    if not ORTOOLS_AVAILABLE: return [] # Cannot format if ortools not loaded
    try:
        total_distance = 0
        distance_dimension = routing.GetDimensionOrDie("Distance")
        for vehicle_id in range(data["num_vehicles"]):
            index = routing.Start(vehicle_id)
            if not routing.IsVehicleUsed(solution, vehicle_id): continue
            rider_id = f"Rider{vehicle_id + 1}"; seq = 1
            route_for_vehicle = []
            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                route_record_id = f"ROUTE_REC_{week_no}_{rider_id}_{seq}"
                route_step = {"RouteRecordID": route_record_id, "WeekNo": week_no, "RiderID": rider_id, "Seq": seq, "LocID": node_index}
                route_for_vehicle.append(route_step)
                index = solution.Value(routing.NextVar(index)); seq += 1
            # Add the end node (depot return)
            end_node_index = manager.IndexToNode(index)
            end_route_record_id = f"ROUTE_REC_{week_no}_{rider_id}_{seq}"
            route_step = {"RouteRecordID": end_route_record_id, "WeekNo": week_no, "RiderID": rider_id, "Seq": seq, "LocID": end_node_index}
            route_for_vehicle.append(route_step)
            route_distance = solution.Value(distance_dimension.CumulVar(index))
            logger.info(f"Chatbot: Formatted route {rider_id}: Seq Len {seq}, Dist {route_distance}m")
            routes_data.extend(route_for_vehicle); total_distance += route_distance
        logger.info(f"Chatbot: Total distance all routes: {total_distance}m")
        return routes_data
    except Exception as e:
        logger.error(f"Chatbot: Error formatting OR-Tools solution: {e}", exc_info=True); return []

def chatbot_generate_routes_tool_internal(num_vehicles: int, week_no: int) -> Dict[str, Any]:
    """Internal logic for generating routes using OR-Tools (Chatbot)."""
    logger.info(f"Chatbot: Internal tool generating routes Week {week_no}, {num_vehicles} vehicles.")
    result = {"status": "error", "message": "Route generation failed.", "routes_data": None, "objective_value": None, "max_route_distance": None}

    if not ORTOOLS_AVAILABLE:
         result["message"] = "OR-Tools library not available. Cannot generate routes."
         logger.error(result["message"])
         return result
    if not isinstance(num_vehicles, int) or num_vehicles <= 0:
        result["message"] = "Number of vehicles must be a positive integer."; return result
    if not isinstance(week_no, int) or week_no <= 0:
        result["message"] = "Week number must be a positive integer."; return result
    if not GOOGLE_MAPS_API_KEY or "YOUR_GOOGLE_MAPS_API_KEY" in GOOGLE_MAPS_API_KEY:
        result["message"] = "Google Maps API key not configured."; return result

    addresses = BASE_ADDRESSES
    if len(addresses) == 0: result["message"] = "No base addresses defined."; return result
    if len(addresses) <= num_vehicles :
        result["message"] = f"Not enough unique addresses ({len(addresses)}) for {num_vehicles} vehicles (need >= {num_vehicles + 1})."
        logger.warning(result["message"]); return result

    distance_matrix = chatbot_create_distance_matrix(addresses, GOOGLE_MAPS_API_KEY)
    if distance_matrix is None:
        result["message"] = "Failed to create distance matrix via Google Maps API."; return result

    data = {"distance_matrix": distance_matrix, "num_vehicles": num_vehicles, "depot": 0}
    try:
        manager = pywrapcp.RoutingIndexManager(len(data["distance_matrix"]), data["num_vehicles"], data["depot"])
        routing = pywrapcp.RoutingModel(manager)
        def distance_callback(from_index, to_index):
            from_node = manager.IndexToNode(from_index); to_node = manager.IndexToNode(to_index)
            if 0 <= from_node < len(data["distance_matrix"]) and 0 <= to_node < len(data["distance_matrix"][from_node]):
                 return data["distance_matrix"][from_node][to_node]
            else: logger.error(f"Invalid distance_callback indices: {from_node}, {to_node}"); return 9999999
        transit_callback_index = routing.RegisterTransitCallback(distance_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)
        dimension_name = "Distance"; routing.AddDimension(transit_callback_index, 0, 3000000, True, dimension_name)
        distance_dimension = routing.GetDimensionOrDie(dimension_name)
        distance_dimension.SetGlobalSpanCostCoefficient(100)
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.FromSeconds(30)
        logger.info("Chatbot: Solving OR-Tools VRP...")
        solution = routing.SolveWithParameters(search_parameters)
    except Exception as e:
        logger.error(f"Chatbot: Error during OR-Tools setup/solving: {e}", exc_info=True)
        result["message"] = f"OR-Tools engine error: {e}"; return result

    if solution:
        logger.info("Chatbot: OR-Tools Solution found.")
        formatted_routes = chatbot_format_solution(data, manager, routing, solution, week_no)
        if not formatted_routes: result["message"] = "Solution found but failed to format routes."; return result
        result["status"] = "success"; result["message"] = f"Successfully generated routes for W{week_no}, {num_vehicles} vehicles."
        result["routes_data"] = formatted_routes; result["objective_value"] = solution.ObjectiveValue()
        max_route_dist = 0
        try:
            for i in range(data["num_vehicles"]):
                 if routing.IsVehicleUsed(solution, i):
                     index = routing.End(i); route_dist = solution.Value(distance_dimension.CumulVar(index))
                     max_route_dist = max(route_dist, max_route_dist)
        except Exception as e: logger.warning(f"Could not calc max route distance: {e}"); max_route_dist = -1
        result["max_route_distance"] = max_route_dist
        logger.info(f"Chatbot: Gen success. Obj: {result['objective_value']}, MaxDist: {max_route_dist}m")
    else:
        status_map = {pywrapcp.ROUTING_NOT_SOLVED:"NOT_SOLVED", pywrapcp.ROUTING_FAIL:"FAIL", pywrapcp.ROUTING_FAIL_TIMEOUT:"TIMEOUT", pywrapcp.ROUTING_INVALID:"INVALID"}
        solver_status_code = routing.status(); solver_status_str = status_map.get(solver_status_code, f"UNKNOWN_{solver_status_code}")
        result["message"] = f"OR-Tools could not find solution. Status: {solver_status_str}."; logger.warning(result["message"])
    return result


# --- Chatbot BigQuery Interaction Logic ---

def chatbot_insert_routes_to_bigquery_tool_internal(routes_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Internal logic to insert generated route data into BigQuery (Chatbot)."""
    table_id = BQ_ROUTES_TABLE_ID
    logger.info(f"Chatbot: Internal tool inserting {len(routes_data)} routes into {table_id}")
    result = {"status": "error", "message": "BQ insert failed.", "rows_inserted": 0, "errors": None}
    if not isinstance(routes_data, list): result["message"] = "Invalid input: routes_data must be list."; return result
    if not routes_data: result["message"] = "No route data to insert."; result["status"] = "success"; return result
    if not table_id or "your-gcp-project" in table_id: result["message"] = "Routes BQ table ID not configured."; return result
    if bq_client is None: result["message"] = "BQ client unavailable."; return result
    try:
        errors = bq_client.insert_rows_json(table_id, routes_data, skip_invalid_rows=False)
        if not errors:
            result["status"] = "success"; result["message"] = f"Inserted {len(routes_data)} route steps into {table_id}."
            result["rows_inserted"] = len(routes_data); logger.info(result["message"])
        else:
            result["message"] = "Errors during BQ insertion."; result["errors"] = errors
            error_details = [f"Row {e.get('index', 'N/A')}: {e.get('errors', 'Unknown')}" for e in errors]
            logger.error(f"BQ Insert Errors:\n" + "\n".join(error_details)); result["message"] += " Details logged."
    except Forbidden as e: result["message"] = f"Permission denied inserting into {table_id}."; logger.error(result["message"], exc_info=True); result["errors"]=[str(e)]
    except NotFound as e: result["message"] = f"BQ table {table_id} not found."; logger.error(result["message"], exc_info=True); result["errors"]=[str(e)]
    except Exception as e: result["message"] = f"Unexpected BQ insertion error: {e}"; logger.error(result["message"], exc_info=True); result["errors"]=[str(e)]
    return result

def chatbot_update_quantity_in_bigquery(project_id: str, dataset_id: str, table_name: str) -> Dict[str, Union[str, int, None]]:
    """Internal logic for updating inventory quantity based on demand (Chatbot)."""
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    logger.info(f"Chatbot: Internal tool updating quantity for table: {full_table_id}")
    result_status = {"status": "error", "message": "Inventory update failed.", "affected_rows": None}
    if not all([project_id, dataset_id, table_name]): result_status["message"] = "Missing project/dataset/table name."; return result_status
    if bq_client is None: result_status["message"] = "BQ client unavailable."; return result_status
    sql_query = f"UPDATE `{full_table_id}` SET Quantity = `Demand__Required_` WHERE `Demand__Required_` > Quantity;"
    logger.info(f"Chatbot: Executing replenishment SQL: {sql_query}")
    try:
        query_job = bq_client.query(sql_query); query_job.result()
        affected_rows = query_job.num_dml_affected_rows
        if affected_rows is not None:
            msg = f"Successfully updated Quantity in {full_table_id}. Affected rows: {affected_rows}"
            logger.info(msg); result_status["status"] = "success"; result_status["message"] = msg; result_status["affected_rows"] = affected_rows
        else:
             msg = f"Update query ran for {full_table_id}, but affected rows count unavailable."
             logger.warning(msg); result_status["status"] = "warning"; result_status["message"] = msg
    except NotFound: msg = f"Error: Table '{full_table_id}' not found."; logger.error(msg); result_status["message"] = msg
    except Forbidden as e: msg = f"Permission denied updating '{full_table_id}'."; logger.error(msg, exc_info=True); result_status["message"] = msg
    except GoogleAPIError as e: msg = f"BQ API error during update: {e}"; logger.error(msg, exc_info=True); result_status["message"] = msg
    except Exception as e: msg = f"Unexpected error during inventory update: {e}"; logger.error(msg, exc_info=True); result_status["message"] = msg
    return result_status


# --- Chatbot NL to SQL Logic ---

def chatbot_get_date_range(user_prompt: str) -> str:
    """Uses Gemini to determine start/end dates from natural language (Chatbot)."""
    logger.info(f"Chatbot: Getting date range from NL: '{user_prompt}'")
    today = datetime.date.today(); today_str = today.strftime("%Y-%m-%d")
    if not VERTEX_AI_INITIALIZED:
        logger.error("Vertex AI unavailable for date parsing."); return json.dumps({"start_date": today_str, "end_date": today_str, "error": "AI unavailable"})
    try:
        nl_sql_model = GenerativeModel(NL_BQ_AGENT_MODEL_ID)
        example_json = json.dumps({"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"})
        prompt = f"Analyze date request: '{user_prompt}'. Today: {today_str} ({today.strftime('%A')}). Consider 'last week', 'this month'. Output ONLY JSON: {example_json}"
        response = nl_sql_model.generate_content(prompt)
        if not response.candidates or not response.candidates[0].content.parts: raise ValueError("AI empty response.")
        response_text = response.candidates[0].content.parts[0].text
        cleaned_response = response_text.strip().replace("```json", "").replace("```", "").strip()
        date_data = json.loads(cleaned_response)
        if "start_date" not in date_data or "end_date" not in date_data: raise ValueError("JSON missing keys.")
        logger.info(f"Chatbot: AI Date range: {date_data}"); return json.dumps(date_data)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Chatbot: Failed parsing AI date response '{cleaned_response}': {e}")
        return json.dumps({"start_date": today_str, "end_date": today_str, "error": "AI parse fail"})
    except Exception as e:
        logger.error(f"Chatbot: Failed NL date range: {e}", exc_info=True)
        return json.dumps({"start_date": today_str, "end_date": today_str, "error": f"Error: {e}"})

def chatbot_parse_and_format_dates(date_range_json_str: str) -> Dict[str, str]:
    """Parses the date range JSON string and formats dates (Chatbot)."""
    logger.debug(f"Chatbot: Parsing date JSON: {date_range_json_str}")
    try:
        data_dict = json.loads(date_range_json_str)
        if 'start_date' not in data_dict or 'end_date' not in data_dict: raise ValueError("Missing keys")
        start_date = pd.to_datetime(data_dict['start_date']).strftime('%Y-%m-%d')
        end_date = pd.to_datetime(data_dict['end_date']).strftime('%Y-%m-%d')
        logger.info(f"Chatbot: Parsed dates - Start: {start_date}, End: {end_date}")
        return {"start_date": start_date, "end_date": end_date}
    except Exception as e:
        logger.error(f"Chatbot: Failed parse/format date JSON '{date_range_json_str}': {e}")
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        return {"start_date": today_str, "end_date": today_str, "error": f"Format error: {e}"}

# --- Chatbot Function Declarations for Gemini Tools ---
# Defined earlier with other constants

# --- Chatbot Internal NL->SQL Query Handler ---
def chatbot_answer_query_from_bq_internal(user_query: str) -> str:
    """Handles NL query, interacts with Gemini+Tools, executes BQ, returns summary (Chatbot)."""
    logger.info(f"Chatbot: Internal NL->SQL processing query: '{user_query}'")
    if bq_client is None: return "Error: BQ client unavailable."
    if not VERTEX_AI_INITIALIZED: return "Error: AI model unavailable."
    available_tables = [REPLENISH_TABLE_NAME, "routes", "order_table", "locations"]
    logger.debug(f"NL->SQL Available Tables: {available_tables}")
    try:
        model = GenerativeModel(NL_BQ_AGENT_MODEL_ID, tools=[nl_sql_tool])
        chat = model.start_chat(response_validation=False)
    except Exception as e: logger.error(f"Chatbot: Failed init Gemini NL->SQL: {e}", exc_info=True); return f"Error: AI init fail ({e})."
    prompt = f"Answer question: '{user_query}' using BQ dataset '{BQ_DATASET_ID}'. Tables: {', '.join(available_tables)}. Steps: 1.Parse dates (`get_parsed_date_range`). 2.ID tables (`get_table` if needed). 3.Construct SELECT SQL for `{PROJECT_ID}.{BQ_DATASET_ID}`. 4.Execute (`sql_query`). 5.Summarize result concisely (no SQL/tables). If no data, say so. If fail, report error. Constraint: SELECT only."
    try:
        response = chat.send_message(prompt)
        while True:
             if not response.candidates or not response.candidates[0].content.parts: logger.warning("NL->SQL: Model response empty."); break
             part = response.candidates[0].content.parts[0]
             if not hasattr(part, 'function_call') or not part.function_call or not part.function_call.name: break # Assume final answer
             function_call = part.function_call; api_response_content = None
             params = {key: value for key, value in function_call.args.items()} if function_call.args else {}
             logger.info(f"NL->SQL: Model call: '{function_call.name}' Params: {params}")
             try:
                 if function_call.name == "get_parsed_date_range":
                     date_json_str = chatbot_get_date_range(params.get("date_phrase", user_query))
                     api_response_content = json.loads(date_json_str)
                 elif function_call.name == "list_datasets": api_response_content = {"datasets": [BQ_DATASET_ID]}
                 elif function_call.name == "list_tables": api_response_content = {"tables": available_tables}
                 elif function_call.name == "get_table":
                     table_name = params.get("table_name")
                     if not table_name: api_response_content = {"error": "Table name missing."}
                     elif table_name not in available_tables: api_response_content = {"error": f"Access denied. Allowed: {', '.join(available_tables)}."}
                     else:
                         full_table_id = f"{PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"
                         try:
                             table_info = bq_client.get_table(full_table_id)
                             schema = [{"name": f.name, "type": str(f.field_type)} for f in table_info.schema]
                             api_response_content = {"table_name": table_name, "description": table_info.description, "schema": schema}
                         except NotFound: api_response_content = {"error": f"Table '{full_table_id}' not found."}
                         except Exception as e_get: api_response_content = {"error": f"Error getting table {full_table_id}: {e_get}"}
                 elif function_call.name == "sql_query":
                     query = params.get("query")
                     if not query: api_response_content = {"error": "SQL query missing."}
                     elif re.search(r'\b(UPDATE|DELETE|INSERT|MERGE|CREATE|DROP|ALTER|TRUNCATE)\b', query, re.IGNORECASE):
                         logger.warning(f"NL->SQL: Denied harmful query: {query}")
                         api_response_content = {"error": "Query denied. SELECT only."}
                     else:
                         logger.info(f"NL->SQL: Executing BQ query: {query}")
                         try:
                             query_job = bq_client.query(query)
                             results_list = [dict(row.items()) for row in query_job.result()]
                             MAX_ROWS_TO_LLM = 50
                             if len(results_list) > MAX_ROWS_TO_LLM:
                                 logger.warning(f"NL->SQL: Truncating BQ results {len(results_list)}->{MAX_ROWS_TO_LLM}")
                                 api_response_content = {"results": results_list[:MAX_ROWS_TO_LLM], "truncated": True, "total_rows_found": len(results_list)}
                             else:
                                 api_response_content = {"results": results_list, "truncated": False, "total_rows_found": len(results_list)}
                             logger.debug(f"NL->SQL: Query success. Rows: {len(results_list)}")
                         except Forbidden as e_sql: logger.error(f"NL->SQL: BQ perm denied: {e_sql}", True); api_response_content = {"error": "Permission denied for BQ query."}
                         except GoogleAPIError as e_sql: logger.error(f"NL->SQL: BQ API error: {e_sql}", True); api_response_content = {"error": f"BQ query failed: {e_sql.message}"}
                         except Exception as e_sql: logger.error(f"NL->SQL: BQ query error: {e_sql}", True); api_response_content = {"error": f"BQ query failed: {e_sql}"}
                 else: api_response_content = {"error": f"Unknown function: {function_call.name}"}; logger.warning(api_response_content["error"])
             except Exception as e_func:
                 logger.error(f"NL->SQL: Error exec func '{function_call.name}': {e_func}", True); api_response_content = {"error": f"Failed exec tool '{function_call.name}': {e_func}"}
             logger.info(f"NL->SQL: Sending response to LLM for func '{function_call.name}'.")
             logger.debug(f"NL->SQL: API Resp Content (preview): {str(api_response_content)[:500]}...")
             response = chat.send_message(Part.from_function_response(name=function_call.name, response={"content": api_response_content}))
        # --- End Loop ---
        if response.candidates and response.candidates[0].content.parts:
             final_part = response.candidates[0].content.parts[0]
             if hasattr(final_part, 'text') and final_part.text:
                 final_response = final_part.text; logger.info(f"NL->SQL: Final summary: {final_response}"); return final_response
             else: logger.error("NL->SQL: Final response part no text."); return "Error: AI gave invalid final answer."
        else: logger.error("NL->SQL: Final response invalid."); return "Error: AI gave invalid final response structure."
    except Exception as e:
        logger.error(f"Chatbot: Error during NL->SQL chat: {e}", exc_info=True); return f"Error processing query: {type(e).__name__}."


# --- Chatbot Tool Wrappers ---
def chatbot_generate_routes_wrapper(num_vehicles: int, week_no: int) -> Union[str, List[Dict[str, Any]]]:
    """Agent Tool: Generates routes and saves them (Chatbot)."""
    logger.info(f"Chatbot Agent Tool: generate_routes_wrapper W{week_no}, {num_vehicles} vehicles.")
    if not isinstance(num_vehicles, int) or num_vehicles <= 0: return "Tool Input Error: 'num_vehicles' positive integer required."
    if not isinstance(week_no, int) or week_no <= 0: return "Tool Input Error: 'week_no' positive integer required."
    gen_result = chatbot_generate_routes_tool_internal(num_vehicles=num_vehicles, week_no=week_no)
    if gen_result.get("status") != "success" or not gen_result.get("routes_data"):
        error_msg = gen_result.get("message", "Route generation failed."); logger.error(f"Chatbot Wrapper: Gen failed: {error_msg}"); return f"Route Generation Error: {error_msg}"
    generated_routes = gen_result["routes_data"]; logger.info(f"Chatbot Wrapper: Generated {len(generated_routes)} steps.")
    logger.info("Chatbot Wrapper: Auto-inserting routes to BQ...")
    insert_result = chatbot_insert_routes_to_bigquery_tool_internal(routes_data=generated_routes)
    if insert_result.get("status") != "success":
        insert_error_msg = insert_result.get("message", "Saving to BQ failed."); logger.error(f"Chatbot Wrapper: Insert fail after gen: {insert_error_msg}")
        return f"Warning: Routes generated ({len(generated_routes)} steps), but saving failed: {insert_error_msg}. Check logs."
    else:
        success_msg = insert_result.get("message", f"Generated/saved {len(generated_routes)} steps."); logger.info(f"Chatbot Wrapper: {success_msg}"); return generated_routes

def chatbot_answer_query_from_bq_wrapper(user_query: str) -> str:
    """Agent Tool: Answers NL questions via BQ (Chatbot)."""
    logger.info(f"Chatbot Agent Tool: answer_query_from_bq_wrapper query: '{user_query}'")
    if not user_query or not isinstance(user_query, str): return "Tool Input Error: Invalid/empty query."
    return chatbot_answer_query_from_bq_internal(user_query=user_query)

def chatbot_run_inventory_replenishment_wrapper() -> str:
    """Agent Tool: Triggers inventory replenishment (Chatbot)."""
    logger.info("Chatbot Agent Tool: run_inventory_replenishment_wrapper called.")
    try:
        result = chatbot_update_quantity_in_bigquery(PROJECT_ID, BQ_DATASET_ID, REPLENISH_TABLE_NAME)
        message = result.get("message", "Replenishment status unknown.")
        # Fulfill specific instruction if successful and rows affected
        if result.get("status") == "success" and result.get("affected_rows", 0) > 0:
             message = "The orders for Inventory replenishment have been created and Inventory is updated."
        elif result.get("status") == "success" and result.get("affected_rows", 0) == 0:
             message = "Inventory replenishment ran, but no items required updating (Quantity >= Demand)."
        elif result.get("status") == "error": message = f"Inventory Replenishment Error: {message}"
        logger.info(f"Chatbot Wrapper: Replenishment result: {message}"); return message
    except Exception as e:
        logger.error(f"Chatbot Wrapper: Error calling replenishment tool: {e}", True); return f"Tool Execution Error: {e}"


# --- Chatbot Agent Definitions ---

# @st.cache_resource # Caching agents can sometimes cause issues
def create_route_generator_agent():
    """Creates the Agno Agent for route generation and saving."""
    logger.info("Chatbot: Creating Route Generator Agent instance...")
    if not VERTEX_AI_INITIALIZED or not GOOGLE_CLOUD_AVAILABLE: return None
    if 'chatbot_generate_routes_wrapper' not in globals(): return None
    try:
        return Agent(name="Route Generation Agent", role="Generate vehicle routes and save to BigQuery.", model=AgnoGemini(id=ROUTE_AGENT_MODEL_ID),
            instructions=["1.ID Week#(int>0) & #Riders(int>0). 2.Ask if missing. 3.Exec `chatbot_generate_routes_wrapper` tool(num_vehicles,week_no). 4.Tool does gen+save. 5.Returns LIST(ok) or STRING(err/warn). 6.If LIST: report 'generated&saved', #steps, sample(RiderID,Seq,LocID). 7.If STR: report msg exactly. 8.NO ask save. 9.NO query/replenish."],
            tools=[chatbot_generate_routes_wrapper], show_tool_calls=True, markdown=True)
    except Exception as e: logger.error(f"Failed create Route Gen Agent: {e}", True); return None

# @st.cache_resource
def create_nl_bigquery_agent():
    """Creates the Agno Agent for answering questions via BigQuery."""
    logger.info("Chatbot: Creating NL BigQuery Agent instance...")
    if not VERTEX_AI_INITIALIZED or not GOOGLE_CLOUD_AVAILABLE: return None
    if 'chatbot_answer_query_from_bq_wrapper' not in globals(): return None
    try:
        return Agent(name="BigQuery NL Query Agent", role=f"Answers questions about supply chain data from BQ dataset '{BQ_DATASET_ID}'. Handles greetings.", model=AgnoGemini(id=NL_BQ_AGENT_MODEL_ID),
            instructions=[f"1.If question re data in BQ '{BQ_DATASET_ID}', exec `chatbot_answer_query_from_bq_wrapper`. 2.Pass full question as `user_query`. 3.Tool does SQL+query+summary. 4.Present tool's text response. 5.NO route gen/save. 6.NO replenish. 7.If asked gen/replenish, say no/specialist. 8.Handle greetings."],
            tools=[chatbot_answer_query_from_bq_wrapper], show_tool_calls=True, markdown=True)
    except Exception as e: logger.error(f"Failed create NL BQ Agent: {e}", True); return None

# @st.cache_resource
def create_replenish_agent():
    """Creates the Agno Agent for inventory replenishment."""
    logger.info("Chatbot: Creating Inventory Replenishment Agent instance...")
    if not VERTEX_AI_INITIALIZED or not GOOGLE_CLOUD_AVAILABLE: return None
    if 'chatbot_run_inventory_replenishment_wrapper' not in globals(): return None
    try:
        return Agent(name="Inventory Replenishment Agent", role=f"Updates inventory quantities in '{REPLENISH_TABLE_ID}' based on demand.", model=AgnoGemini(id=REPLENISH_AGENT_MODEL_ID),
            instructions=[f"1.Purpose: trigger replenish in BQ `{REPLENISH_TABLE_ID}`. 2.If asked replenish/update stock, exec `chatbot_run_inventory_replenishment_wrapper`(no args). 3.Tool does `SET Q=D WHERE D>Q`. 4.Report tool's exact status msg. 5.**If tool confirms updates, return EXACTLY: 'The orders for Inventory replenishment have been created and Inventory is updated.'** 6.NO ask confirm. 7.NO ask params. 8.NO route gen/query. 9.If not clear replenish req, clarify/state purpose."],
            tools=[chatbot_run_inventory_replenishment_wrapper], show_tool_calls=True, markdown=True)
    except Exception as e: logger.error(f"Failed create Replenish Agent: {e}", True); return None


# --- Chatbot Team Definition ---
# @st.cache_resource
def create_router_team():
    """Creates the Agno Team to route requests to the appropriate agent."""
    logger.info("Chatbot: Creating Router Team instance...")
    if not GOOGLE_CLOUD_AVAILABLE: return None # Need Agno/Vertex
    route_agent = create_route_generator_agent(); nl_agent = create_nl_bigquery_agent(); replenish_agent = create_replenish_agent()
    members = [agent for agent in [route_agent, nl_agent, replenish_agent] if agent is not None]
    if not members: logger.error("Chatbot: No agents created, cannot make team."); return None
    if not VERTEX_AI_INITIALIZED: logger.error("Chatbot: Vertex AI unavailable for router model."); return None
    try:
        return Team(name="Supply Chain Task Router", mode="route", model=AgnoGemini(id=TEAM_ROUTER_MODEL_ID), members=members, show_tool_calls=False, markdown=True,
            instructions=["Route user request to best agent: 1.Gen/Create Routes->'Route Generation Agent'. 2.Query/Ask data(orders,inventory,past routes)/Greeting->'BigQuery NL Query Agent'. 3.Replenish/Update Inventory/Stock->'Inventory Replenishment Agent'. 4.Save Data: Routes->RouteGen, Other->NLQuery/Clarify. 5.Prioritize action. Select ONE agent."],
            show_members_responses=True)
    except Exception as e: logger.error(f"Failed create Router Team: {e}", True); return None

# --- Chatbot Async Runner ---
async def get_team_response(team: Optional[Team], query: str) -> str:
    """Runs the user query through the Agno Team asynchronously (Chatbot)."""
    logger.info(f"Chatbot: Running query via team: '{query}'")
    if team is None: logger.error("Chatbot: get_team_response called with no team."); return "Error: AI team unavailable."
    try:
        response_object = await team.arun(query); response_content = ""
        if isinstance(response_object, str): response_content = response_object
        elif hasattr(response_object, 'content'):
             content_data = response_object.content
             if content_data is None: response_content = "Agent ran but gave no content."; logger.warning(f"Chatbot: Agent content None for query: {query}")
             elif isinstance(content_data, str): response_content = content_data
             elif isinstance(content_data, list):
                try:
                    if content_data and isinstance(content_data[0], dict):
                         preview_data = content_data[:5]; json_str = json.dumps(preview_data, indent=None)
                         response_content = f"Data (first {len(preview_data)}/{len(content_data)}):\n```json\n{json_str}\n```"
                         if len(content_data) > 5: response_content += "\n(More data available)"
                    else: response_content = f"Data: {str(content_data)[:1000]}" + ("..." if len(str(content_data)) > 1000 else "")
                except Exception as json_e: logger.warning(f"Chatbot: JSON format list fail: {json_e}"); response_content = f"Received list data: {str(content_data)[:1000]}" + ("..." if len(str(content_data)) > 1000 else "")
             elif isinstance(content_data, dict):
                 try: json_str = json.dumps(content_data, indent=2); response_content = f"```json\n{json_str}\n```"
                 except Exception as json_e: logger.warning(f"Chatbot: JSON format dict fail: {json_e}"); response_content = f"Received dict data: {str(content_data)}"
             else: try: response_content = str(content_data); except Exception: response_content = "Error processing agent response type."
        elif response_object is None: response_content = "No response from AI team."; logger.warning(f"Chatbot: Team returned None for query: {query}")
        else: try: response_content = f"Unexpected resp type: {type(response_object).__name__}. Content: {str(response_object)}"; except Exception: logger.error(f"Chatbot: Failed converting unknown resp type: {type(response_object)}"); response_content = "Error processing AI response."
        if not response_content: response_content = "Empty response from agent."; logger.warning(f"Chatbot: Empty final response for query: {query}")
        log_preview = (response_content[:500] + '...') if len(response_content) > 500 else response_content; logger.info(f"Chatbot: Team final response preview: {log_preview}"); return response_content
    except ModelProviderError as e: logger.error(f"Chatbot: Model Provider Error: {e.message}", False); status = f" (Status: {e.status_code})" if hasattr(e, 'status_code') and e.status_code else ""; return f"AI model error: {e.message}{status}."
    except Forbidden as e: logger.error(f"Chatbot: Permission Error query='{query}': {e}", True); return f"Permission Error during request. Check GCP permissions. Details: {e}"
    except Exception as e:
        logger.error(f"Chatbot: Error team execution query='{query}': {e}", True)
        if "pydantic_core._pydantic_core.ValidationError" in str(e): logger.error(f"Chatbot: Pydantic Error: {e}", True); return "Internal Error: Tool result validation failed. Check logs."
        if "cannot run loop" in str(e).lower(): logger.error(f"Chatbot: Asyncio loop issue: {e}", True); return "Internal Error: Async operations failed. Restart app?"
        return f"Error processing request: {type(e).__name__}. Check logs."


# --- Chatbot UI Function ---
def chatbot_run_ui():
    """Sets up and runs the Streamlit UI for the Chatbot view."""
    st.title("💬 Supply Chain Chatbot") # Title specific to this view
    st.caption(f"Interact via natural language | Router: {TEAM_ROUTER_MODEL_ID} | Dataset: {BQ_DATASET_ID}")

    # Chatbot specific sidebar content - Displayed when chatbot view is active
    # st.sidebar.divider() # Already have dividers in main app
    # st.sidebar.subheader("Chatbot Examples") ... # Can keep these here

    # Initialize chat history in session state if it doesn't exist
    if "chatbot_messages" not in st.session_state:
        st.session_state.chatbot_messages = [{"role": "assistant", "content": "Hello! How can I assist with your supply chain tasks today?"}]

    # Display chat messages from history
    for message in st.session_state.chatbot_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"]) # Use markdown for better formatting

    # Get user input via chat interface
    if prompt := st.chat_input("Ask (e.g., 'Generate routes for week 30 for 2 riders')..."):
        # Add user message to history and display
        st.session_state.chatbot_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Process user prompt and get assistant response
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            message_placeholder.markdown("Thinking...")
            logger.info(f"Chatbot User prompt: {prompt}")

            # Create the team (needs error handling)
            team_router = create_router_team()
            if team_router is None:
                assistant_response = "Error: Chatbot AI team initialization failed. Cannot process request. Check logs."
                logger.error(assistant_response)
                message_placeholder.error(assistant_response)
            else:
                # Run the query asynchronously using asyncio.run()
                assistant_response = asyncio.run(get_team_response(team_router, prompt))
                message_placeholder.markdown(assistant_response) # Display final response

            # Add assistant response (or error message) to history
            st.session_state.chatbot_messages.append({"role": "assistant", "content": assistant_response})


# =============================================================================
# 8. UI Rendering Functions (Top Level Control)
# =============================================================================

def render_dashboard():
    """Renders the Dashboard UI."""
    logger.info("Rendering Dashboard View")

    # --- Prerequisite Check ---
    if bq_client is None:
        st.error("Dashboard cannot be displayed: BigQuery client failed to initialize.", icon="☁️")
        return

    # Apply dashboard-specific CSS
    apply_dashboard_styling()

    # --- Load Data ---
    df_orders_raw, df_inventory_bq_raw, df_history_demand, df_forecast_demand = None, None, None, None
    with st.spinner("Loading dashboard data..."):
        # Load data using dashboard functions
        df_orders_raw = load_excel(ORDER_EXCEL_PATH, "Orders")
        df_inventory_bq_raw = load_bigquery_inventory(bq_client)
        df_history_demand = load_historical_demand_data()
        df_forecast_demand = load_bigquery_forecast(bq_client)

    # --- Process Data ---
    df_inventory_cleaned, df_orders, df_orders_loaded_successfully = None, None, False
    with st.spinner("Processing dashboard data..."):
        df_inventory_cleaned = clean_and_validate_inventory(df_inventory_bq_raw)
        # --- Order Processing Logic ---
        load_error_message = ""
        if df_orders_raw is not None:
            try:
                df_orders = df_orders_raw.copy()
                if 'Order Date' in df_orders.columns:
                    df_orders['Order Date'] = pd.to_datetime(df_orders['Order Date'], errors='coerce')
                    initial_rows = len(df_orders)
                    df_orders.dropna(subset=['Order Date'], inplace=True)
                    if len(df_orders) < initial_rows: st.warning(f"Removed {initial_rows - len(df_orders)} orders with invalid dates.", icon="⚠️")
                else: st.warning("Order data missing 'Order Date'.", icon="⚠️")
                price_cols = ['Unit Price (USD)', 'Total Price (USD)']
                for col in price_cols:
                    if col in df_orders.columns:
                        if not pd.api.types.is_numeric_dtype(df_orders[col]):
                             df_orders[col] = pd.to_numeric(df_orders[col], errors='coerce')
                        df_orders[col].fillna(0, inplace=True)
                    else: st.warning(f"Order data missing '{col}'.", icon="⚠️")
                if 'Order Status' not in df_orders.columns: st.error("Order data missing 'Order Status'.", icon="❗"); df_orders_loaded_successfully = False
                elif 'Product Name' not in df_orders.columns: st.warning("Order data missing 'Product Name'.", icon="❗"); df_orders_loaded_successfully = True
                elif df_orders.empty and not df_orders_raw.empty: st.warning("Orders empty after processing.", icon="📄"); df_orders_loaded_successfully = True
                elif df_orders_raw.empty: st.info("Order file was empty.", icon="📄"); df_orders_loaded_successfully = True
                else: df_orders_loaded_successfully = True
            except Exception as e: st.error(f"Error processing orders: {e}", icon="❌"); load_error_message = str(e); traceback.print_exc(); df_orders = None; df_orders_loaded_successfully = False
        else: st.info("Order data could not be loaded.", icon="ℹ️"); df_orders_loaded_successfully = False
        # --- End Order Processing ---

    # --- Dashboard Header ---
    header_icon_url = "https://media-hosting.imagekit.io/d4d2d070da764e7a/supply-chain%20(1).png?Expires=1838385562&Key-Pair-Id=K2ZIVPTIP2VGHC&Signature=rI6qlVGN1aOU6B2kLFPU~ZPYiyXFC8eEqvDp~Tnjf9-XnMk2GI~9QYhtG9yS1n12nQ~Xg9H5UCw-uByoFNwmMbAZhvoQYrQAmREiud-IzIQKBncPOB9XVmOxnDCGBvXd6xmC7z~eJV~cjrmaqXqUL4tRVYQQ330kNVuI3Qg2MB9DbjeYuPiHGsqGTOPSDBQw8~Upmcf2oB3whSq-7Fg5R~LYSLmSRFPAalm2Anlw8fxbiCbeVp0yZy6uGG2YSnZ5BSFDHEPL2E4MsYRYL-2HySHoTflBe3D2fJJGQsiIKp8QnZ8UQE0toJaxIZCgTjfrwhtpbL-V3DI4YQ3Jdwxo5w__"
    st.markdown(f"""
    <div class="custom-header-container" style="background-color: #1a73e8; padding: 1rem 1.5rem; border-radius: 8px; margin-bottom: 1.5rem; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15); text-align: center;">
        <h1 style="color: #ffffff; margin-bottom: 0.5rem; font-size: 2.0em; font-weight: 600; display: flex; align-items: center; justify-content: center;">
            <img src="{header_icon_url}" alt="Logistics Icon" style="margin-right: 15px; height: 40px; vertical-align: middle;"> Supply Chain Intelligence - Dashboard
        </h1>
        <p style="color: #e3f2fd; font-size: 1.0rem; margin-bottom: 0; line-height: 1.4;">Integrated Forecasting, Inventory, Orders, and Routes.</p>
    </div>
    """, unsafe_allow_html=True)

    # --- Dashboard Tabs ---
    tab_demand, tab_inventory, tab_orders, tab_route = st.tabs([
        "📈 Sales Forecast", "📦 Inventory", "🛒 Orders", "🗺️ Rider Route"
    ])

    # --- Render Demand Forecast Tab ---
    with tab_demand:
        st.markdown('<h2 class="tab-header">Sales Forecast Analysis</h2>', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">Historical Data</h3>', unsafe_allow_html=True)
        if df_history_demand is not None:
            if not df_history_demand.empty: st.dataframe(df_history_demand, use_container_width=True, hide_index=True); st.caption(f"Source: `{os.path.basename(HISTORY_CSV_PATH)}`")
            else: st.info(f"Historical demand file empty.", icon="📄")
        else: st.warning(f"Could not load historical demand.", icon="⚠️")
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<h3 class="sub-header">Forecast Data (from BigQuery)</h3>', unsafe_allow_html=True)
        if df_forecast_demand is not None:
            if not df_forecast_demand.empty: st.dataframe(df_forecast_demand, use_container_width=True, hide_index=True); st.caption(f"Source: BQ Table `{BQ_FORECAST_TABLE_ID.split('.')[-1]}`")
            else: st.info(f"Forecast data table empty.", icon="📄")
        else: st.warning(f"Could not load forecast data.", icon="☁️")

    # --- Render Inventory Tab ---
    with tab_inventory:
        st.markdown('<h2 class="tab-header">Inventory Management</h2>', unsafe_allow_html=True)
        if df_inventory_cleaned is not None and not df_inventory_cleaned.empty:
            total_items = len(df_inventory_cleaned); quantities = df_inventory_cleaned['Quantity']; demands = df_inventory_cleaned['Demand (Required)']
            valid_mask = quantities.notna() & demands.notna(); valid_q = quantities[valid_mask]; valid_d = demands[valid_mask]
            short = (valid_d > valid_q).sum(); exact = (valid_d == valid_q).sum(); surplus = (valid_d < valid_q).sum()
            st.markdown('<div class="card-container">', True); c1,c2,c3,c4=st.columns(4)
            with c1: st.markdown(f'<div class="info-card"><span class="card-label">SKUs</span><span class="card-value">{total_items}</span></div>', True)
            with c2: st.markdown(f'<div class="warning-card"><span class="card-label">Shortages</span><span class="card-value">{short}</span></div>', True)
            with c3: st.markdown(f'<div class="neutral-card"><span class="card-label">Exact Match</span><span class="card-value">{exact}</span></div>', True)
            with c4: st.markdown(f'<div class="success-card"><span class="card-label">Surplus</span><span class="card-value">{surplus}</span></div>', True)
            st.markdown('</div>', True); st.markdown("<br>", True)
            st.markdown("""<div class="legend-container">... (Paste Legend HTML here) ...</div>""", True) # Shortened for brevity
            st.markdown('<h3 class="sub-header">Inventory Details</h3>', True)
            st.dataframe(df_inventory_cleaned.style.apply(highlight_demand, axis=1), use_container_width=True, hide_index=True)
            st.caption(f"Source: BQ Table `{BQ_PRODUCTS_TABLE_ID.split('.')[-1]}`")
        elif df_inventory_cleaned is not None and df_inventory_cleaned.empty: st.info("Inventory empty after cleaning.", icon="🧹")
        elif df_inventory_bq_raw is not None: st.warning("Inventory loaded but failed cleaning.", icon="⚠️")
        else: st.error(f"Inventory data failed to load.", icon="❌")

    # --- Render Order Management Tab ---
    with tab_orders:
        st.markdown('<h2 class="tab-header">Order Management</h2>', unsafe_allow_html=True)
        if not df_orders_loaded_successfully: st.error(f"Error loading/processing order data.", icon="🚨"); # ... (Error details) ...
        elif df_orders is None or df_orders.empty: st.info(f"No valid orders found.", icon="📄")
        else:
            total_orders = len(df_orders)
            total_value = pd.to_numeric(df_orders.get('Total Price (USD)', 0), errors='coerce').sum()
            delivered = df_orders[df_orders['Order Status'] == 'Delivered'].shape[0] if 'Order Status' in df_orders else 0
            pending = df_orders[df_orders['Order Status'] == 'Pending'].shape[0] if 'Order Status' in df_orders else 0
            st.markdown('<div class="card-container">', True); co1,co2,co3,co4=st.columns(4)
            with co1: st.markdown(f'<div class="info-card"><span class="card-label">Total Orders</span><span class="card-value">{total_orders}</span></div>', True)
            with co2: st.markdown(f'<div class="success-card"><span class="card-label">Delivered</span><span class="card-value">{delivered}</span></div>', True)
            with co3: st.markdown(f'<div class="neutral-card"><span class="card-label">Pending</span><span class="card-value">{pending}</span></div>', True)
            with co4: st.markdown(f'<div class="info-card"><span class="card-label">Total Value</span><span class="card-value">${total_value:,.2f}</span></div>', True)
            st.markdown('</div>', True); st.markdown('<div class="section-divider"></div>', True)
            st.markdown('<h3 class="sub-header">Order Details</h3>', True)
            st.dataframe(df_orders, use_container_width=True, hide_index=True)
            st.caption(f"Source: `{os.path.basename(ORDER_EXCEL_PATH)}`")

    # --- Render Rider Route Tab ---
    with tab_route:
        st.markdown('<h2 class="tab-header">Rider Route Visualization</h2>', unsafe_allow_html=True)
        if bq_client is None: st.warning("BQ client unavailable.", icon="☁️")
        else:
            weeks_riders_df = get_available_weeks_riders(bq_client); sel_week, sel_rider = None, None
            if weeks_riders_df is None or weeks_riders_df.empty: st.warning("Could not load Weeks/Riders.", icon="⚠️")
            else:
                cs1, cs2 = st.columns(2)
                with cs1:
                    avail_weeks = sorted(weeks_riders_df['WeekNo'].dropna().unique().astype(int), reverse=True)
                    if avail_weeks: sel_week = st.selectbox("Select Week:", avail_weeks, index=0, key="dash_week_sel")
                    else: st.warning("No weeks found.")
                with cs2:
                    if sel_week is not None:
                        riders_week = sorted(weeks_riders_df[weeks_riders_df['WeekNo'] == sel_week]['RiderID'].dropna().unique())
                        if riders_week: sel_rider = st.selectbox("Select Rider:", riders_week, index=0, key="dash_rider_sel")
                        else: st.warning(f"No riders for Week {sel_week}.")
                    else: st.info("Select week first.")
            st.markdown('<div class="section-divider"></div>', True)
            if sel_week is not None and sel_rider:
                st.markdown(f"#### Route Map: W{sel_week}, R{sel_rider}")
                route_seq_df, locs_df, route_details_df, route_map_df = None, None, None, None
                with st.spinner("Loading route data..."): route_seq_df = get_route_data(bq_client, sel_week, sel_rider)
                if route_seq_df is None or route_seq_df.empty: st.warning("No route sequence found.", icon="📍")
                else:
                    loc_ids = route_seq_df['LocID'].dropna().unique().tolist()
                    if not loc_ids: st.warning("No valid LocIDs in route.", icon="🤨")
                    else:
                        with st.spinner("Loading locations..."): locs_df = get_location_data(bq_client, loc_ids)
                        if locs_df is None or locs_df.empty: st.error("Location details not found.", icon="❌")
                        else:
                            route_details_df = pd.merge(route_seq_df.sort_values('Seq'), locs_df, on='LocID', how='left')
                            missing = route_details_df['Lat'].isnull() | route_details_df['Long'].isnull(); n_miss = missing.sum()
                            if n_miss > 0: st.warning(f"{n_miss} stops missing coords.", icon="⚠️"); route_map_df = route_details_df.dropna(subset=['Lat', 'Long']).copy()
                            else: route_map_df = route_details_df.copy()
                            if route_map_df.empty: st.warning("No stops with valid coords.", icon="🙁")
                            else:
                                # --- Map Generation ---
                                osrm_path = None; # ... (Call get_osrm_route(route_map_df[['Long', 'Lat']])) ...
                                path_layer_data = None; path_color = [255,165,0,180]; path_width = 3
                                if osrm_path: path_layer_data = pd.DataFrame({'path':[osrm_path]}); path_color=[0,128,255,200]; path_width=4
                                elif route_map_df.shape[0] >= 2: st.info("Drawing straight lines."); lines = route_map_df[['Long','Lat']].values.tolist(); path_layer_data=pd.DataFrame({'path':[lines]})
                                else: st.info("Only one point.")
                                # ... (Define get_icon_data function as before) ...
                                def get_icon_data(loc_id, seq, max_s, min_s):
                                     is_dc = str(loc_id)==str(DC_LOC_ID); is_start=seq==min_s; is_end=seq==max_s
                                     url=DC_PIN_URL if is_dc else STORE_PIN_URL; mult=1.6 if is_dc and (is_start or is_end) else (1.3 if is_dc else 1.0)
                                     h = int(PIN_HEIGHT*mult); w = int(PIN_WIDTH*mult); aY = int(h*PIN_ANCHOR_Y_FACTOR)
                                     return {"url":url, "width":w, "height":h, "anchorY":aY}
                                min_s = route_map_df['Seq'].min(); max_s = route_map_df['Seq'].max()
                                route_map_df['icon_data'] = route_map_df.apply(lambda r: get_icon_data(r['LocID'], r['Seq'], max_s, min_s), axis=1)
                                # ... (Calculate initial_view_state as before) ...
                                try:
                                    lat_c, lon_c = route_map_df['Lat'].mean(), route_map_df['Long'].mean()
                                    lat_r, lon_r = route_map_df['Lat'].max()-route_map_df['Lat'].min(), route_map_df['Long'].max()-route_map_df['Long'].min()
                                    zoom = max(min(11 - np.log(max(lat_r, lon_r, 0.01)), 15), 8)
                                    view_state = pdk.ViewState(lat_c, lon_c, zoom, pitch=45)
                                except Exception: view_state = pdk.ViewState(39.8, -98.6, 4, pitch=0)
                                layers = []
                                if path_layer_data is not None: layers.append(pdk.Layer("PathLayer", path_layer_data, get_path="path", get_color=path_color, width_min_pixels=path_width))
                                layers.append(pdk.Layer("IconLayer", route_map_df, get_icon="icon_data", get_position=["Long", "Lat"], size_scale=10, pickable=True, auto_highlight=True, highlight_color=[255,255,0,200]))
                                tooltip_html = "<b>📍 {LocName}</b><br/>#{Seq} ({LocID})<br/>{Lat:.4f}, {Long:.4f}"
                                tooltip={"html": f"<div style='background:rgba(30,30,30,0.8);color:white;padding:8px 12px;border-radius:4px;'>{tooltip_html}</div>"}
                                st.pydeck_chart(pdk.Deck(map_style="mapbox://styles/mapbox/light-v10", initial_view_state=view_state, layers=layers, tooltip=tooltip), True)
                                # --- Route Summary & Details ---
                                st.markdown('<div class="section-divider"></div>', True); st.subheader("Route Summary")
                                # ... (Paste route summary generation loop from previous answer) ...
                                if route_details_df is not None and not route_details_df.empty:
                                     summary_items = []; # ... (Generate summary_items list) ...
                                     st.markdown("\n".join(summary_items))
                                st.markdown('<div class="section-divider"></div>', True); st.markdown("#### Route Stop Details")
                                if route_details_df is not None and not route_details_df.empty: st.dataframe(route_details_df[['Seq', 'LocID', 'LocName', 'Lat', 'Long']], True, hide_index=True)
                                else: st.info("No details available.")
            elif not (weeks_riders_df is None or weeks_riders_df.empty): st.info("Select Week/Rider.", icon="👆")


def render_chatbot():
    """Renders the Chatbot UI."""
    logger.info("Rendering Chatbot View")

    # --- Prerequisite Checks ---
    chatbot_ready = True
    if not bq_client: st.error("Chatbot unavailable: BigQuery Client failed.", icon="☁️"); chatbot_ready = False
    if not VERTEX_AI_INITIALIZED: st.error("Chatbot unavailable: Vertex AI failed.", icon="🤖"); chatbot_ready = False
    if not GOOGLE_CLOUD_AVAILABLE: st.error("Chatbot unavailable: Google Cloud libraries missing.", icon="📦"); chatbot_ready = False
    if not ORTOOLS_AVAILABLE: st.warning("Chatbot Warning: OR-Tools missing. Route generation disabled.", icon="⚠️") # Warn, don't stop
    if not GOOGLE_MAPS_API_KEY or "YOUR_GOOGLE_MAPS_API_KEY" in GOOGLE_MAPS_API_KEY:
         st.warning("Chatbot Warning: Google Maps API Key missing. Route generation disabled.", icon="🗺️") # Warn, don't stop

    if chatbot_ready:
        # Call the chatbot's main UI function
        chatbot_run_ui()
    else:
        st.warning("Chatbot is not fully operational due to configuration or initialization errors listed above.")

# =============================================================================
# 10. Main Application Logic (View Selection)
# =============================================================================

st.sidebar.title("Navigation")
app_mode = st.sidebar.radio(
    "Select View:",
    ("Dashboard", "Chatbot"),
    key="app_mode_selector",
    help="Switch between the visual dashboard and the conversational chatbot."
)

# --- Display Status in Sidebar ---
st.sidebar.divider()
st.sidebar.markdown("---")
st.sidebar.header("System Status")
st.sidebar.markdown(f"**BigQuery Client:** {'✅ Connected' if bq_client else '❌ Failed'}")
st.sidebar.markdown(f"**Vertex AI Models:** {'✅ Initialized' if VERTEX_AI_INITIALIZED else '❌ Failed/Skipped'}")
st.sidebar.markdown(f"**OR-Tools:** {'✅ Available' if ORTOOLS_AVAILABLE else '❌ Missing'}")
st.sidebar.markdown(f"**Dashboard Libs:** {'✅ Available' if DASHBOARD_LIBS_AVAILABLE else '⚠️ Missing Opt.'}")
st.sidebar.markdown(f"**GCP Project:** `{PROJECT_ID}`")
st.sidebar.markdown(f"**BQ Dataset:** `{BQ_DATASET}`")


# --- Conditionally Render the Selected View ---
if app_mode == "Dashboard":
    logger.info("Switching to Dashboard view")
    render_dashboard()
elif app_mode == "Chatbot":
    logger.info("Switching to Chatbot view")
    render_chatbot()
else:
    # Fallback (optional, default to Dashboard maybe?)
    logger.warning(f"Unknown app_mode selected: {app_mode}. Defaulting to Dashboard.")
    st.sidebar.error("Invalid view selected!")
    render_dashboard()

# --- Optional Footer ---
st.markdown("---")
st.caption("Integrated Supply Chain Hub | Powered by Streamlit, Google Cloud & OR-Tools")
