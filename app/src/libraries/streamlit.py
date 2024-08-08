import json
from sys import exit

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum
import snowflake.permissions as permission
import pydeck as pdk
import h3

st.set_page_config(layout="wide")
session = get_active_session()


def create_hexagon_map(df_hex_top_locations):
    df_hex_top_locations['latitude'] = df_hex_top_locations['H3_HEX_RESOLUTION_6'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex_top_locations['longitude'] = df_hex_top_locations['H3_HEX_RESOLUTION_6'].apply(lambda x: h3.h3_to_geo(x)[1])

    n = len(df_hex_top_locations)

    # Calculate threshold indices for N/3 groups
    top_threshold = n // 3
    middle_threshold = 2 * (n // 3)

    # Function to determine the color based on TOTAL_SALES_USD rank
    def get_color(index):
        if index < top_threshold:
            return [0, 144, 189, 160]    # Dark Green
        elif index < middle_threshold:
            return [0, 191, 255, 160]  # deep sky blue
        else:
            return [135, 206, 250, 160] # light sky blue

    df_hex_top_locations['color'] = df_hex_top_locations.index.map(lambda x: get_color(x))

    # Underlying Data
    # Create the pydeck H3HexagonLayer
    layer = pdk.Layer(
        'H3HexagonLayer',
        df_hex_top_locations,
        get_hexagon='H3_HEX_RESOLUTION_6',
        get_fill_color='color',
        get_elevation='TOTAL_SALES_USD * 0.0001',
        elevation_scale=1,
        extruded=True,
        pickable=True,
    )

    # Define the view state with a higher zoom level
    view_state = pdk.ViewState(
        latitude=df_hex_top_locations['latitude'].mean(),
        longitude=df_hex_top_locations['longitude'].mean(),
        zoom=9,  # Increased zoom level for more details
        pitch=45,
    )

    r = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/light-v11",
        tooltip={
            "html": "<b>H3 Index:</b> {H3_HEX_RESOLUTION_6}<br>"
            "<b>Number of Top 50 Locations:</b> {NUMBER_OF_TOP_50_LOCATIONS}<br>"
            "<b>Customer Loyalty Visitor Count:</b> {CUSTOMER_LOYALTY_VISITOR_COUNT}<br>"
            "<b>Total Sales (USD):</b> {TOTAL_SALES_USD}",
            "style": {"color": "white"},
        },
    )
    st.pydeck_chart(r)


def create_point_map(df):
    center_point = json.loads(df['GEOMETRIC_CENTER_POINT'][0])
    center_coords = [center_point['coordinates'][0], center_point['coordinates'][1]]

    df['KILOMETER_FROM_TOP_SELLING_CENTER_STR'] = df['KILOMETER_FROM_TOP_SELLING_CENTER'].astype(str)

    # Scatterplotlayer for the farthest points
    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_radius=100,
        get_fill_color=[255, 0, 0, 100],
        pickable=True,
        auto_highlight=True,
    )

    # Scatterplotlayer for the center point: displaying bigger circles at the center point for levels of 1, 2, 3 closeness
    circles_data = [
        {"position": center_coords, "radius": 16000, "color": [255, 50,  50, 40]},
        {"position": center_coords, "radius": 8000,  "color": [255, 255, 50, 40]},
        {"position": center_coords, "radius": 4000,  "color": [50,  255, 50, 40]},
    ]

    circles_scatter = pdk.Layer(
        "ScatterplotLayer",
        data=circles_data,
        get_position="position",
        get_radius="radius",
        get_fill_color="color",
    )

    lines = pdk.Layer(
        "LineLayer",
        data=df,
        get_source_position=["LONGITUDE", "LATITUDE"],
        get_target_position=center_coords,
        get_color=[160, 50, 40, 128],
        get_width=2,
        pickable=True,
    )

    view_state = pdk.ViewState(
        latitude=center_coords[1],
        longitude=center_coords[0],
        zoom=8,
        pitch=0,
    )

    r = pdk.Deck(
        layers=[circles_scatter, scatter, lines, ],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/light-v11",
        tooltip = {
            "html": "<b>Name:</b> {LOCATION_NAME} <br/> <b>Distance(km):</b> {KILOMETER_FROM_TOP_SELLING_CENTER_STR}",
            "style": {
                "backgroundColor": "steelblue",
                "color": "white"
            }
        },
    )

    st.pydeck_chart(r)

    # disposals
    df.drop(columns=['KILOMETER_FROM_TOP_SELLING_CENTER_STR'], inplace=True, axis=1)


def load_app(orders_table):
    with st.spinner("Loading sale locations insights, please wait...."):
        #  NOTE: Create a view to be used for further queries
        _ = session.sql(f"""
            CREATE OR REPLACE VIEW orders_v
            COMMENT = 'Tasty Bytes Order Detail View'
            AS
            SELECT
                DATE(o.order_ts) AS date,
                o.*,
                cpg.* EXCLUDE (location_id, region, phone_number, country)
            FROM {orders_table} o
            JOIN frostbyte_tb_safegraph_s cpg ON o.location_id = cpg.location_id;
        """).collect()

        df_location_farther_from_top_point = session.sql(f"""
            WITH _CENTER_POINT AS (
                WITH _top_10_locations AS (
                    SELECT TOP 10
                        o.location_id,
                        ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
                        SUM(o.price) AS total_sales_usd
                        FROM ORDERS_V o
                        WHERE primary_city = 'Paris'
                        GROUP BY o.location_id, o.latitude, o.longitude
                        ORDER BY total_sales_usd DESC
                )
                SELECT
                    ST_COLLECT(tl.geo_point) AS collect_points,
                    ST_CENTROID(collect_points) AS geometric_center_point
                    FROM _top_10_locations tl
            ), _paris_locations AS (
                SELECT DISTINCT
                    location_id,
                    location_name,
                    ST_MAKEPOINT(longitude, latitude) AS geo_point,
                    latitude,
                    longitude
                    FROM ORDERS_V
                    WHERE primary_city = 'Paris'
            )
            SELECT TOP 50
                location_id,
                location_name,
                ROUND(ST_DISTANCE(geo_point, TO_GEOGRAPHY(_CENTER_POINT.geometric_center_point))/1000,2) AS kilometer_from_top_selling_center,
                longitude,
                latitude,
                _CENTER_POINT.geometric_center_point
            FROM _paris_locations,_CENTER_POINT
            ORDER BY kilometer_from_top_selling_center DESC"""
        ).to_pandas()

        df_hex_top_locations = session.sql(f"""
            WITH _top_50_locations AS (
                SELECT TOP 50
                    location_id,
                    ARRAY_SIZE(ARRAY_UNIQUE_AGG(customer_id)) AS customer_loyalty_visitor_count,
                    H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_integer_resolution_6,
                    H3_LATLNG_TO_CELL_STRING(latitude, longitude, 7) AS h3_hex_resolution_6,
                    SUM(price) AS total_sales_usd
                    FROM orders_v
                    WHERE primary_city = 'Paris'
                    GROUP BY ALL
                    ORDER BY total_sales_usd DESC
            )
            SELECT
                h3_hex_resolution_6,
                COUNT(DISTINCT location_id) AS number_of_top_50_locations,
                SUM(customer_loyalty_visitor_count) AS customer_loyalty_visitor_count,
                SUM(total_sales_usd) AS total_sales_usd
                FROM _top_50_locations
                GROUP BY ALL
                ORDER BY total_sales_usd DESC
        """).to_pandas()

        with st.container():
            col1, col2 = st.columns(2,gap='small')
            with col1:
                st.subheader("distance from top selling locations")
                create_point_map(df_location_farther_from_top_point)
                st.table(data=df_location_farther_from_top_point)

            with col2:
                st.subheader("top hexagons with sales and locations")
                create_hexagon_map(df_hex_top_locations)
                st.table(data=df_hex_top_locations)


orders_reference_associations = permission.get_reference_associations("order_table")
if len(orders_reference_associations) == 0:
    permission.request_reference("order_table")
    exit(0)


st.title("Sale location insights")
orders_table = "reference('order_table')"
load_app(orders_table)
