footer_logo_url = convert_image_base64("logo_footer.png")
if footer_logo_url: # Only render if image loaded successfully
    st.markdown(
        f"""
        <footer class="footer">
            <span style="font-weight: bold; color: #0056b3;"><img alt="hcl_logo" src="{footer_logo_url}" /></span>
            <span style="display: block; text-align: center; width: 100%;" >Copyright © 2025 HCL Technologies Limited</span>
        </footer>
        """,
        unsafe_allow_html=True,
    )
else:
     # Fallback text footer if image fails
     st.markdown(
        """
        <footer class="footer">
            <span style="font-weight: bold; color: #0056b3;">HCLTech</span>
            <span style="display: block; text-align: center; width: 100%;" >Copyright © 2025 HCL Technologies Limited</span>
        </footer>
        """,
        unsafe_allow_html=True,
     )


     
