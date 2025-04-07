# main_app.py
import streamlit as st
import base64
import os

# --- Page Configuration (Main App) ---
# Sets the overall browser tab title/icon for the landing page
st.set_page_config(
    page_title="Supply Chain Hub",
    page_icon="🔗",  # You can use emojis or URLs
    layout="wide",
    initial_sidebar_state="expanded" # Keep sidebar open
)

# --- Custom CSS for Main Page Styling ---
st.markdown("""
<style>
    /* Add styles from previous examples if desired, or keep it simple */
    .main-container {
        padding: 2rem 3rem;
        background-color: #f9f9f9;
        border-radius: 10px;
    }
    .welcome-header {
        color: #1E88E5; /* Blue */
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .welcome-subheader {
        color: #555;
        text-align: center;
        margin-bottom: 2.5rem;
        font-size: 1.1em;
    }
    .feature-card {
        background-color: #ffffff;
        padding: 1.5rem;
        border-radius: 8px;
        box-shadow: 0 3px 10px rgba(0,0,0,0.1);
        margin-bottom: 1.5rem;
        height: 100%;
        display: flex; flex-direction: column;
    }
    .feature-card h3 { color: #1E88E5; margin-bottom: 1rem; }
    .feature-card p { color: #333; line-height: 1.6; flex-grow: 1;}
    .feature-card ul { padding-left: 20px; margin-bottom: 1rem; flex-grow: 1;}
    .feature-card li { margin-bottom: 0.5rem; color: #444;}
    .nav-prompt {
        text-align: center; font-size: 1.2em; color: #0D47A1; /* Darker Blue */
        margin-top: 2rem; font-weight: 500;
    }
    .nav-arrow { font-size: 1.5em; vertical-align: middle; }
</style>
""", unsafe_allow_html=True)

# --- Main Content ---
st.markdown('<div class="main-container">', unsafe_allow_html=True)

st.markdown("<h1 class='welcome-header'>🚀 Welcome to the Supply Chain Hub!</h1>", unsafe_allow_html=True)
st.markdown("<p class='welcome-subheader'>Navigate through the integrated Dashboard and Chatbot tools using the sidebar.</p>", unsafe_allow_html=True)

# --- Feature Overview ---
col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown("""
    <div class="feature-card">
        <h3>📊 Dashboard</h3>
        <p>Get a visual overview of your supply chain:</p>
        <ul>
            <li>Sales Forecast Analysis</li>
            <li>Inventory Status (vs. Demand)</li>
            <li>Order Management Metrics</li>
            <li>Rider Route Visualization</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="feature-card">
        <h3>💬 Chatbot</h3>
        <p>Interact with your data conversationally:</p>
        <ul>
            <li>Ask Natural Language Questions</li>
            <li>Generate Optimized Routes</li>
            <li>Trigger Inventory Replenishment</li>
            <li>Retrieve Quick Summaries</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<p class='nav-prompt'><span class='nav-arrow'>⬅️</span> Select a tool from the sidebar to begin.</p>", unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True) # Close main-container

st.markdown("---")
st.caption("Integrated Supply Chain Platform")
