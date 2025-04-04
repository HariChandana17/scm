# --- START: Rider Route Tab Block (Hiding TooltipName from final table) ---
with tab_route:
    st.markdown('<h2 class="tab-header">Rider Route Visualization</h2>', unsafe_allow_html=True)

    if bq_client is None:
        st.warning("BigQuery connection unavailable. Route visualization disabled.", icon="☁️")
    else:
        # --- Selection Controls ---
        # (Selection code remains the same)
        weeks_riders_df = get_available_weeks_riders(bq_client)

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
                        "Select Week:", available_weeks, index=0, key="route_week_selector",
                        help="Select the week number for the route.")
            with col_select2:
                selected_rider = None
                if selected_week is not None:
                    riders_in_week = sorted(
                        weeks_riders_df[weeks_riders_df['WeekNo'] == selected_week]['RiderID'].dropna().unique())
                    if not riders_in_week:
                        st.warning(f"No riders found for Week {selected_week}.")
                    else:
                        selected_rider = st.selectbox(
                            "Select Rider:", riders_in_week, key="route_rider_selector",
                            help="Select the rider ID for the route.")
                else:
                    st.info("Select a week to see available riders.")

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True) # Separator

        # --- Map and Details Display ---
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
                         locations_df = get_location_data(bq_client, [str(loc) for loc in unique_loc_ids])

                    if locations_df.empty:
                        st.error(f"Could not find location details for the route stops.", icon="❌")
                    else:
                        # Merge route sequence with location coordinates
                        rider_route_df['LocID_str'] = rider_route_df['LocID'].astype(str)
                        locations_df['LocID_str'] = locations_df['LocID'].astype(str)
                        route_details_df = pd.merge(
                            rider_route_df.sort_values(by='Seq'), locations_df,
                            on='LocID_str', how='left'
                        ).drop(columns=['LocID_str'])

                        # Check for missing coordinates
                        missing_coords = route_details_df['Lat'].isnull().sum() + route_details_df['Long'].isnull().sum()
                        if missing_coords > 0:
                             st.warning(f"{missing_coords // 2} locations in the route are missing coordinates.", icon="⚠️")
                             route_details_df.dropna(subset=['Lat', 'Long'], inplace=True)

                        if route_details_df.empty:
                             st.warning("No locations with valid coordinates found for this route.", icon="🙁")
                        else:
                            # --- Add Custom Tooltip Name Column ---
                            # (Logic remains the same, requires DC_LOC_ID = '0' defined above)
                            first_seq_is_dc = False
                            if not route_details_df.empty:
                                 first_loc_id = str(route_details_df.iloc[0]['LocID'])
                                 if first_loc_id == DC_LOC_ID: first_seq_is_dc = True

                            def get_tooltip_display_name(row, dc_id, first_is_dc):
                                loc_id_str = str(row['LocID'])
                                seq = row['Seq']
                                if loc_id_str == dc_id: return "Distribution Center"
                                else:
                                    outlet_num = seq - 1 if first_is_dc and seq > 1 else seq
                                    if not first_is_dc and seq == 1: outlet_num = 1
                                    return f"Outlet {outlet_num}"
                            route_details_df['TooltipName'] = route_details_df.apply(
                                lambda row: get_tooltip_display_name(row, DC_LOC_ID, first_seq_is_dc), axis=1)

                            # --- Fetch Actual Road Route from OSRM ---
                            # (Logic remains the same)
                            with st.spinner("Fetching road directions from OSRM..."):
                                actual_route_path = get_osrm_route(route_details_df[['Long', 'Lat']])

                            # --- Prepare path data for PyDeck ---
                            # (Logic remains the same)
                            path_layer_data = None
                            path_color = [255, 165, 0, 180]
                            if actual_route_path:
                                path_layer_data = pd.DataFrame({'path': [actual_route_path]})
                                path_color = [0, 128, 255, 200]
                            elif route_details_df.shape[0] >= 2 :
                                st.info("Could not fetch road directions. Drawing straight lines between stops.")
                                straight_line_path = route_details_df[['Long', 'Lat']].values.tolist()
                                path_layer_data = pd.DataFrame({'path': [straight_line_path]})
                            else:
                                st.info("Only one valid point, cannot draw a path.")

                            # --- Define Icon Data ---
                            # (Logic remains the same, requires DC_LOC_ID = '0')
                            def get_icon_data(loc_id, seq, max_seq):
                                loc_id_str = str(loc_id)
                                is_dc = (loc_id_str == DC_LOC_ID)
                                is_start = (seq == route_details_df['Seq'].min())
                                is_end = (seq == max_seq)
                                icon_url = DC_PIN_URL if is_dc else STORE_PIN_URL
                                size_multiplier = 1.5 if is_dc and (is_start or is_end) else (1.2 if is_dc else 1.0)
                                return {"url": icon_url, "width": int(PIN_WIDTH * size_multiplier),
                                        "height": int(PIN_HEIGHT * size_multiplier),
                                        "anchorY": int(PIN_HEIGHT * size_multiplier * PIN_ANCHOR_Y_FACTOR)}
                            max_sequence = route_details_df['Seq'].max()
                            route_details_df['icon_data'] = route_details_df.apply(
                                lambda row: get_icon_data(row['LocID'], row['Seq'], max_sequence), axis=1)

                            # --- PyDeck Rendering ---
                            # (Logic remains the same)
                            try:
                                initial_latitude = route_details_df['Lat'].mean()
                                initial_longitude = route_details_df['Long'].mean()
                                initial_view_state = pdk.ViewState(
                                    latitude=initial_latitude, longitude=initial_longitude,
                                    zoom=11, pitch=45, bearing=0)
                            except Exception:
                                initial_view_state = pdk.ViewState(
                                    latitude=35.1495, longitude=-90.0490, zoom=10, pitch=30)
                            layers = []
                            if path_layer_data is not None:
                                layers.append(pdk.Layer(
                                    "PathLayer", data=path_layer_data, get_path="path",
                                    get_color=path_color, width_min_pixels=5, pickable=False))
                            layers.append(pdk.Layer(
                                "IconLayer", data=route_details_df, get_icon="icon_data",
                                get_position=["Long", "Lat"], get_size='icon_data.height',
                                size_scale=1, pickable=True, auto_highlight=True,
                                highlight_color=[255, 255, 0, 150]))
                            tooltip = {
                                "html": """
                                <div style='background-color: rgba(0,0,0,0.7); color: white; padding: 8px 12px; border-radius: 5px; font-family: sans-serif; font-size: 0.9em;'>
                                    <b>{TooltipName}</b><br/>
                                    Stop #: {Seq}<br/>
                                    ID: {LocID}<br/>
                                    Original Name: {LocName}<br/>
                                    Coords: {Lat:.4f}, {Long:.4f}
                                </div>""",
                                "style": {"backgroundColor": "rgba(0,0,0,0)", "color": "white"}
                            }
                            st.pydeck_chart(pdk.Deck(
                                map_style="mapbox://styles/mapbox/light-v10",
                                initial_view_state=initial_view_state, layers=layers, tooltip=tooltip
                            ), use_container_width=True)

                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

                            # --- Route Summary Text ---
                            # (Logic remains the same)
                            st.subheader("Route Summary")
                            summary_items = []
                            first_loc_id_summary = str(route_details_df.iloc[0]['LocID']) if not route_details_df.empty else None
                            for index, row in route_details_df.iterrows():
                                loc_name = row['LocName']
                                loc_id = row['LocID']
                                seq = row['Seq']
                                prefix = ""
                                icon = "📍"
                                if seq == route_details_df['Seq'].min() and str(loc_id) == DC_LOC_ID:
                                    prefix = f"**Start (DC):** "; icon = "🏭"
                                elif seq == route_details_df['Seq'].max() and str(loc_id) == DC_LOC_ID and seq != route_details_df['Seq'].min():
                                     prefix = f"**End (Return DC):** "; icon = "🏭"
                                elif seq == route_details_df['Seq'].min():
                                    prefix = f"**Start (Stop 1):** "; icon = "🏁"
                                elif seq == route_details_df['Seq'].max():
                                     stop_num_end = seq - 1 if first_loc_id_summary == DC_LOC_ID else seq
                                     prefix = f"**End (Stop {stop_num_end}):** "; icon = "🏁"
                                else:
                                    stop_num = seq - 1 if first_loc_id_summary == DC_LOC_ID else seq
                                    prefix = f"**Stop {stop_num}:** "
                                summary_items.append(f"* {icon} {prefix} {loc_name} (`{loc_id}`)")
                            st.markdown("\n".join(summary_items))

                            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

                            # --- Route Details Table ---
                            st.markdown("#### Route Stop Details")
                            # ------------------------------------------------------------------
                            # --- START: CHANGE - Remove 'TooltipName' from display list ---
                            # ------------------------------------------------------------------
                            display_cols = ['Seq', 'LocID', 'LocName', 'Lat', 'Long'] # Removed 'TooltipName'
                            # ------------------------------------------------------------------
                            # --- END: CHANGE ---
                            # ------------------------------------------------------------------
                            display_cols_exist = [col for col in display_cols if col in route_details_df.columns]
                            st.dataframe(
                                route_details_df[display_cols_exist].reset_index(drop=True),
                                use_container_width=True,
                                hide_index=True # Cleaner look
                                )

        elif selected_week is None or selected_rider is None:
            st.info("Select a Week and Rider above to view the route details and map.", icon="👆")

# --- END: Rider Route Tab Block ---
