# scm

# --- Main Header ---
st.markdown('<div class="main-header"><h1 class="main-title">Supply Chain Operations Hub</h1></div>', unsafe_allow_html=True)

import streamlit as st

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
    font-weight: 600; /* Slightly bolder */
    display: flex;
    align-items: center;
}
.custom-header-container h1 img {
    margin-right: 15px; /* Space between icon and text */
    height: 40px; /* Control icon size */
}
.custom-header-container p {
    color: #333; /* Dark grey text */
    font-size: 1.1rem;
    margin-bottom: 0;
}
</style>
""", unsafe_allow_html=True)

# You'll need an icon image file (e.g., icon.png) in the same directory or specify a path
# Or use an online SVG URL
icon_url = "https://img.icons8.com/fluency/48/000000/delivery-logistics.png" # Example icon URL

st.markdown(f"""
<div class="custom-header-container">
    <h1><img src="{icon_url}" alt="Logistics Icon"> Supply Chain Intelligence Hub</h1>
    <p>Drive efficiency with integrated Sales Forecasting, Inventory Management, Order Tracking, and Route Optimization.</p>
</div>
""", unsafe_allow_html=True)

# --- Rest of your app ---
st.write("App content starts here...")
