# --- HCLTech Custom CSS ---
# Suppress the hamburger menu and default footer
# NOTE: This CSS is applied FIRST to hide defaults.
custom_css = """
<style>
/* Base Streamlit adjustments from HCL code */
.st-emotion-cache-1r4qj8v{background-color: rgb(239 239 239);} /* Might be overridden by body style later */
.stAppHeader{ display:none;} /* Hides Streamlit's own header */
.st-emotion-cache-yw8pof{ max-width:1024px !important; padding:0 12px 80px !important; margin:0 auto; } /* Adjust main container width/padding */
.st-emotion-cache-0{min-height : 100vh; }
.st-emotion-cache-1104ytp h1{ font-size:28px !important; font-weight:normal;}
.stVerticalBlock.st-emotion-cache-1isgx0k.eiemyj3,
.stElementContainer.element-container.st-emotion-cache-1v6sb1a.eiemyj1,
.stMarkdown{ width:100% !important;}
.st-emotion-cache-1104ytp.e121c1cl0 > div,
.stFileUploader { max-width:1000px !important;width:100% !important; margin:0 auto;}

/* Hide default Streamlit elements */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;} /* Hides default Streamlit footer, allows custom one */

/* HCL Custom Header */
.header {
    background: linear-gradient(90deg, #0044cc, #007bff);
    color: white;
    padding: 15px 20px;
    font-size: 25px; /* Consider adjusting if too large */
    font-weight: regular;
    /* --- MODIFIED --- */
    width: 100%;           /* Occupy full viewport width */
    max-width: 1024px;     /* But limit to content max-width */
    /* --- REMOVED margin: -16px -11px 0; --- */
    text-align: left;
    border-bottom: 2px solid #ccc;
    box-sizing: border-box;
    position: fixed;       /* Keep fixed */
    top: 0;
    left: 50%;             /* Center the 1024px block */
    transform: translateX(-50%);
    z-index: 9998;         /* Ensure it's above content */
}
.header img{ margin-right:5px; vertical-align: middle; height: 30px;} /* Control header image size */

/* HCL Custom Footer */
.footer {
    background-color: #f8f9fa;
    color: #333;
    padding: 15px 20px;
    font-size: 14px;
    /* --- KEPT --- */
    width: 100%;           /* Occupy full viewport width */
    max-width:1024px;      /* But limit to content max-width */
    border-top: 2px solid #ccc;
    box-sizing: border-box;
    display: flex;
    justify-content: space-between;
    align-items: center;
    position: fixed;       /* Keep fixed */
    bottom: 0;
    left: 50%;             /* Center the 1024px block */
    transform: translateX(-50%);
    visibility:visible !important; /* Ensure visibility */
    z-index: 9999;         /* Ensure it's above other elements */
}
.footer img{ vertical-align: middle; height: 20px; margin-right: 5px;} /* Control footer image size */
</style>
"""

# --- HCLTech Image Conversion Function ---
# (Keep the convert_image_base64 function as it was)
def convert_image_base64(image_path):
    # ... function code ...
    """Return a base64 encoded string of an image from the local file system"""
    try:
        with open(image_path, "rb") as image_file: # Open in binary mode 'rb'
            encoded_string = base64.b64encode(image_file.read()).decode()
            # Simple check for image type based on extension
            if image_path.lower().endswith(".png"): mime_type = "image/png"
            elif image_path.lower().endswith((".jpg", ".jpeg")): mime_type = "image/jpeg"
            elif image_path.lower().endswith(".gif"): mime_type = "image/gif"
            elif image_path.lower().endswith(".svg"): mime_type = "image/svg+xml"
            else: mime_type = "image/png" # Default to PNG
            data_url = f"data:{mime_type};base64,{encoded_string}"
            return data_url
    except FileNotFoundError:
        # Avoid showing error directly in header/footer space if possible
        print(f"Header/Footer Image Error: File not found at '{image_path}'.")
        # Optionally: st.warning(f"Image not found: {image_path}", icon="🖼️")
        return None # Return None if file not found
    except Exception as e:
        print(f"Error encoding image '{image_path}': {e}")
        # Optionally: st.error(f"Error encoding image '{image_path}': {e}", icon="⚙️")
        return None

# --- Apply HCLTech Custom CSS (Hides defaults, defines .header/.footer) ---
st.markdown(custom_css, unsafe_allow_html=True)

# --- Render HCLTech Custom Header (Fixed at Top) ---
# (Keep the header rendering code as it was)
header_logo_url = convert_image_base64("logo_header.png")
header_content = "HCLTech | Supply Chain Intelligence Hub" # Fallback text
if header_logo_url:
     header_content = f'<img alt="hcl_logo" src="{header_logo_url}" /> |  Supply Chain Intelligence Hub'

st.markdown(
    f"""
    <header class="header">
        {header_content}
    </header>
    """,
    unsafe_allow_html=True,
)

# --- THE REST OF YOUR CODE (APP_STYLE, apply_styling, data loading, your header, tabs, etc.) ---
# ... (No changes needed in the rest of the application logic or APP_STYLE's body padding) ...
# Make sure your APP_STYLE still includes the body padding:
# body {
#     /* ... other styles ... */
#     padding-top: 80px !important;  /* Adjust as needed */
#     padding-bottom: 80px !important; /* Adjust as needed */
# }


# --- Render HCLTech Footer (AT THE VERY END) ---
# (Keep the footer rendering code as it was)
footer_logo_url = convert_image_base64("logo_footer.png")
footer_span_content = '<span style="font-weight: bold; color: #0056b3;">HCLTech</span>' # Fallback
if footer_logo_url:
    footer_span_content = f'<span style="font-weight: bold; color: #0056b3;"><img alt="hcl_logo" src="{footer_logo_url}" /></span>'

st.markdown(
    f"""
    <footer class="footer">
        {footer_span_content}
        <span style="display: block; text-align: center; width: 100%;" >Copyright © 2025 HCL Technologies Limited</span>
    </footer>
    """,
    unsafe_allow_html=True,
 )
