# --- START: Rider Route Tab Block (with DC Pin and Tooltip modifications) ---
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
                            # ------------------------------------------------------------------
                            # --- START: CHANGE 1 - Add Custom Tooltip Name Column ---
                            # ------------------------------------------------------------------
                            # Determine if the very first stop in the sequence is the DC
                            first_seq_is_dc = False
                            if not route_details_df.empty:
                                 # Use .iloc[0] to get the first row after sorting by Seq
                                 first_loc_id = str(route_details_df.iloc[0]['LocID'])
                                 if first_loc_id == DC_LOC_ID:
                                      first_seq_is_dc = True

                            # Function to generate the desired tooltip name
                            def get_tooltip_display_name(row, dc_id, first_is_dc):
                                loc_id_str = str(row['LocID'])
                                seq = row['Seq'] # Seq is Int64Dtype, comparison should be fine

                                if loc_id_str == dc_id:
                                     return "Distribution Center"
                                else:
                                    # Calculate the 'Outlet' number based on sequence
                                    # If DC is stop 1, then stop 2 is Outlet 1, stop 3 is Outlet 2 etc.
                                    # If DC is not stop 1 (or not present), then stop 1 is Outlet 1, stop 2 is Outlet 2 etc.
                                    outlet_num = seq - 1 if first_is_dc and seq > 1 else seq
                                    # Handle the edge case where the first stop is NOT the DC (its seq is 1, outlet num is 1)
                                    if not first_is_dc and seq == 1:
                                        outlet_num = 1

                                    return f"Outlet {outlet_num}"

                            # Apply the function to create the new column
                            # Make sure DC_LOC_ID is defined correctly above
                            route_details_df['TooltipName'] = route_details_df.apply(
                                lambda row: get_tooltip_display_name(row, DC_LOC_ID, first_seq_is_dc), axis=1
                            )
                            # ------------------------------------------------------------------
                            # --- END: CHANGE 1 ---
                            # ------------------------------------------------------------------


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
                            # !!! CRITICAL CHECK AREA !!!
                            def get_icon_data(loc_id, seq, max_seq):
                                # !!! Ensure DC_LOC_ID ('LOC0') exactly matches your data in BigQuery !!!
                                # !!! Ensure DC_PIN_URL and STORE_PIN_URL point to different colored icons !!!
                                is_dc = (str(loc_id) == DC_LOC_ID)
                                is_start = (seq == route_details_df['Seq'].min())
                                is_end = (seq == max_seq)

                                # Use RED pin for DC, BLUE pin for Stores/Outlets
                                icon_url = DC_PIN_URL if is_dc else STORE_PIN_URL

                                # Optional: Make DC icons slightly larger, especially start/end
                                size_multiplier = 1.5 if is_dc and (is_start or is_end) else (1.2 if is_dc else 1.0)

                                return {
                                    "url": icon_url, # This assigns the correct color based on is_dc
                                    "width": int(PIN_WIDTH * size_multiplier),
                                    "height": int(PIN_HEIGHT * size_multiplier),
                                    "anchorY": int(PIN_HEIGHT * size_multiplier * PIN_ANCHOR_Y_FACTOR),
                                    }

                            max_sequence = route_details_df['Seq'].max() # Calculate max sequence once
                            route_details_df['icon_data'] = route_details_df.apply(
                                lambda row: get_icon_data(row['LocID'], row['Seq'], max_sequence), axis=1
                            )
                            # !!! END CRITICAL CHECK AREA !!!


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

                            # Icon Layer (No changes needed here, it uses the 'icon_data' column)
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

                            # ------------------------------------------------------------------
                            # --- START: CHANGE 2 - Update Tooltip HTML ---
                            # ------------------------------------------------------------------
                            tooltip = {
                                "html": """
                                <div style='background-color: rgba(0,0,0,0.7); color: white; padding: 8px 12px; border-radius: 5px; font-family: sans-serif; font-size: 0.9em;'>
                                    <b>{TooltipName}</b><br/> <!-- Use the new column -->
                                    Stop #: {Seq}<br/>
                                    ID: {LocID}<br/>
                                    Original Name: {LocName}<br/> <!-- Keep original name for reference if needed -->
                                    Coords: {Lat:.4f}, {Long:.4f}
                                </div>
                                """,
                                "style": { # Style block often has limited effect, rely on inline or CSS class
                                     "backgroundColor": "rgba(0,0,0,0)",
                                     "color": "white"
                                 }
                            }
                            # ------------------------------------------------------------------
                            # --- END: CHANGE 2 ---
                            # ------------------------------------------------------------------


                            # Render the map (pass the updated tooltip)
                            st.pydeck_chart(pdk.Deck(
                                map_style="mapbox://styles/mapbox/light-v10", # Light style
                                initial_view_state=initial_view_state,
                                layers=layers,
                                tooltip=tooltip # Ensure the updated tooltip is passed
                            ), use_container_width=True)

                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

                            # --- Route Summary Text ---
                            # This section already has similar logic for display names,
                            # ensure it remains consistent or update if necessary.
                            # It should generally work as is.
                            st.subheader("Route Summary")
                            summary_items = []
                            # (Re-calculate first_loc_id here for the summary scope)
                            first_loc_id_summary = str(route_details_df.iloc[0]['LocID']) if not route_details_df.empty else None

                            for index, row in route_details_df.iterrows():
                                loc_name = row['LocName'] # Use original name here for clarity in summary
                                loc_id = row['LocID']
                                seq = row['Seq']
                                prefix = ""
                                icon = "📍" # Default stop icon

                                if seq == route_details_df['Seq'].min() and str(loc_id) == DC_LOC_ID:
                                    prefix = f"**Start (DC):** "
                                    icon = "🏭"
                                elif seq == route_details_df['Seq'].max() and str(loc_id) == DC_LOC_ID and seq != route_details_df['Seq'].min():
                                     prefix = f"**End (Return DC):** "
                                     icon = "🏭"
                                elif seq == route_details_df['Seq'].min(): # Start but not DC
                                    prefix = f"**Start (Stop 1):** "
                                    icon = "🏁"
                                elif seq == route_details_df['Seq'].max(): # End but not DC
                                     stop_num_end = seq - 1 if first_loc_id_summary == DC_LOC_ID else seq
                                     prefix = f"**End (Stop {stop_num_end}):** "
                                     icon = "🏁"
                                else: # Intermediate stop
                                    stop_num = seq - 1 if first_loc_id_summary == DC_LOC_ID else seq
                                    prefix = f"**Stop {stop_num}:** "

                                summary_items.append(f"* {icon} {prefix} {loc_name} (`{loc_id}`)")

                            st.markdown("\n".join(summary_items))


                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

                            # --- Route Details Table ---
                            st.markdown("#### Route Stop Details")
                            # Add the TooltipName to the table for verification/consistency
                            display_cols = ['Seq', 'LocID', 'LocName', 'TooltipName', 'Lat', 'Long']
                            # Filter display_cols to only those that actually exist in route_details_df
                            display_cols_exist = [col for col in display_cols if col in route_details_df.columns]
                            st.dataframe(
                                route_details_df[display_cols_exist].reset_index(drop=True),
                                use_container_width=True,
                                hide_index=True # Cleaner look
                                )

        elif selected_week is None or selected_rider is None:
            st.info("Select a Week and Rider above to view the route details and map.", icon="👆")

# --- END: Rider Route Tab Block ---
