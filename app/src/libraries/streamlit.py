import json
from sys import exit

import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permission
import pydeck as pdk
import h3

st.set_page_config(layout="wide")
session = get_active_session()


MAX_NUMBER_OF_LOCATIONS = 50


def create_hexagon_map(df_hex_top_locations, input_elevation_metric, input_color_division):
    df_hex_top_locations['latitude'] = df_hex_top_locations['H3_HEX_RESOLUTION_6'].apply(lambda x: h3.h3_to_geo(x)[0])
    df_hex_top_locations['longitude'] = df_hex_top_locations['H3_HEX_RESOLUTION_6'].apply(lambda x: h3.h3_to_geo(x)[1])

    n = len(df_hex_top_locations)

    # Calculate threshold indices for N/3 groups
    # Determine thresholds based on the color division, divide index into "color_division" groups

    number_of_locations_in_each_group = n // input_color_division
    thresholds = []
    next_threshold = number_of_locations_in_each_group
    while True:
        if next_threshold >= n:
            thresholds.append(n - 1)
            break
        thresholds.append(next_threshold)
        next_threshold += number_of_locations_in_each_group

    base_red = 0
    base_green = 40
    base_blue = 120
    base_alpha = 160
    increment_red = 15
    increment_green = 30
    increment_blue = 20

    def get_color(index):
        for i, threshold in enumerate(thresholds):
            if index <= threshold:
                new_red = min(base_red + increment_red * i, 255)
                new_green = min(base_green + increment_green * i, 255)
                new_blue = min(base_blue + increment_blue * i, 255)
                return [new_red, new_green, new_blue, base_alpha]

    df_hex_top_locations['color'] = df_hex_top_locations.index.map(lambda x: get_color(x))

    if input_elevation_metric == 'TOTAL_SALES_USD':
        elevatopn_factor = 0.0001
    elif input_elevation_metric == 'CUSTOMER_LOYALTY_VISITOR_COUNT':
        elevatopn_factor = 0.1
    else:
        elevatopn_factor = 1

    # Underlying Data
    # Create the pydeck H3HexagonLayer
    layer = pdk.Layer(
        'H3HexagonLayer',
        df_hex_top_locations,
        get_hexagon='H3_HEX_RESOLUTION_6',
        get_fill_color='color',
        get_elevation=f'{input_elevation_metric} * {elevatopn_factor}',
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

    # disposals
    df_hex_top_locations.drop(columns=['color'], inplace=True, axis=1)


def create_point_map(df, point_radius_metric, enable_lines):
    center_point = json.loads(df['GEOMETRIC_CENTER_POINT'][0])
    center_coords = [center_point['coordinates'][0], center_point['coordinates'][1]]

    df['KILOMETER_FROM_TOP_SELLING_CENTER_STR'] = df['KILOMETER_FROM_TOP_SELLING_CENTER'].astype(str)

    min_radius = 75
    max_radius = 300

    # We'll need to repsent points in scatter plot layer based on different metrics presented in the data
    # so the given metric values should be normalized to a range of min_radius to max_radius

    # Normalizing the values (let's say we are using TOTAL_SALES_USD as the metric)
    df[point_radius_metric] = df[point_radius_metric].astype(float)
    df['radius'] = (df[point_radius_metric] - df[point_radius_metric].min()) / (df[point_radius_metric].max() - df[point_radius_metric].min())
    df['radius'] = df['radius'] * (max_radius - min_radius) + min_radius

    # Scatterplotlayer for the farthest points
    scatter = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_radius="radius",
        get_fill_color=[255, 0, 0, 100],
        pickable=True,
        auto_highlight=True,
    )

    # Scatterplotlayer for the center point: displaying bigger circles at the center point for levels of 1, 2, 3 closeness
    # 1 radius is approx 1 meters so we can use KILOMETER_FROM_TOP_SELLING_CENTER to represent the radius by multiplying it with 1000
    min_distance_meters = df['KILOMETER_FROM_TOP_SELLING_CENTER'].min() * 1000
    mean_distance_meters = df['KILOMETER_FROM_TOP_SELLING_CENTER'].mean() * 1000
    max_distance_meters = df['KILOMETER_FROM_TOP_SELLING_CENTER'].max() * 1000

    circles_data = [
        {"position": center_coords, "radius": max_distance_meters, "color": [255, 50,  50, 40]},
        {"position": center_coords, "radius": mean_distance_meters,  "color": [255, 255, 50, 40]},
        {"position": center_coords, "radius": min_distance_meters,  "color": [50,  255, 50, 40]},
    ]

    circles_scatter = pdk.Layer(
        "ScatterplotLayer",
        data=circles_data,
        get_position="position",
        get_radius="radius",
        get_fill_color="color",
    )

    if enable_lines:
        # Lines connecting the farthest points to the centera
        lines = pdk.Layer(
            "LineLayer",
            data=df,
            get_source_position=["LONGITUDE", "LATITUDE"],
            get_target_position=center_coords,
            get_color=[160, 50, 40, 128],
            get_width=2,
            pickable=True,
        )
    else:
        lines = None

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
    df.drop(columns=['radius'], inplace=True, axis=1)


@st.cache_data
def create_orders_view(orders_table):
    return session.sql(f"""
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


@st.cache_data
def load_locations():
    df_locations = session.sql(f"""
        SELECT DISTINCT
            country,
            city,
        FROM frostbyte_tb_safegraph_s
    """).to_pandas()
    return df_locations


@st.cache_data
def load_farthest_locations(input_city_selection, input_number_of_locations):
    return session.sql(f"""
        WITH _CENTER_POINT AS (
            WITH _top_10_locations AS (
                SELECT TOP 10
                    o.location_id,
                    ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point,
                    SUM(o.price) AS total_sales_usd
                FROM ORDERS_V o
                WHERE primary_city = '{input_city_selection}'
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
                longitude,
                SUM(price) AS total_sales_usd,
                ARRAY_SIZE(ARRAY_UNIQUE_AGG(customer_id)) AS customer_loyalty_visitor_count
            FROM ORDERS_V
            WHERE primary_city = '{input_city_selection}'
            GROUP BY location_id, location_name, latitude, longitude
        )
        SELECT TOP {input_number_of_locations}
            location_id,
            location_name,
            ROUND(ST_DISTANCE(geo_point, TO_GEOGRAPHY(_CENTER_POINT.geometric_center_point))/1000,2) AS kilometer_from_top_selling_center,
            longitude,
            latitude,
            _CENTER_POINT.geometric_center_point,
            total_sales_usd,
            customer_loyalty_visitor_count
        FROM _paris_locations,_CENTER_POINT
        ORDER BY kilometer_from_top_selling_center DESC"""
    ).to_pandas()


@st.cache_data
def load_hex_top_locations(input_city_selection, input_number_of_locations):
    return session.sql(f"""
        WITH _top_50_locations AS (
            SELECT TOP {input_number_of_locations}
                location_id,
                ARRAY_SIZE(ARRAY_UNIQUE_AGG(customer_id)) AS customer_loyalty_visitor_count,
                H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_integer_resolution_6,
                H3_LATLNG_TO_CELL_STRING(latitude, longitude, 7) AS h3_hex_resolution_6,
                SUM(price) AS total_sales_usd
            FROM orders_v
            WHERE primary_city = '{input_city_selection}'
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


def load_app(orders_table):
    with st.spinner("Loading sale locations insights, please wait...."):
        #  NOTE: Create a view to be used for further queries
        _ = create_orders_view(orders_table)

        df_locations = load_locations()

        input_country_selection = st.selectbox(
            "Select a country",
            sorted(df_locations['COUNTRY'].unique().tolist()),
        )

        if not input_country_selection:
            st.warning("Please select a country")
            return

        input_city_selection = st.selectbox(
            "Select a city",
            df_locations[df_locations['COUNTRY'] == input_country_selection]['CITY'].unique().tolist(),
        )

        if not input_city_selection:
            st.warning("Please select a city")
            return

        input_number_of_locations = st.number_input("Enter number of locations", min_value=1, max_value=MAX_NUMBER_OF_LOCATIONS, value=MAX_NUMBER_OF_LOCATIONS)

        if not input_number_of_locations:
            st.warning("Please enter a number of locations")
            return

        if input_number_of_locations > MAX_NUMBER_OF_LOCATIONS:
            st.warning(f"Please enter a number of locations less than or equal to {MAX_NUMBER_OF_LOCATIONS}")
            return

        if input_number_of_locations < 1:
            st.warning("Please enter a number of locations greater than 0")
            return

        df_location_farther_from_top_point = load_farthest_locations(input_city_selection, input_number_of_locations)
        df_hex_top_locations = load_hex_top_locations(input_city_selection, input_number_of_locations)

        with st.container():
            col1, col2 = st.columns(2,gap='small')
            with col1:
                st.subheader("Locations Furthest Away from Top Selling Center Point")
                input_enable_lines = st.checkbox("Enable lines", value=True)
                st.text(" ")
                st.text(" ")
                st.text(" ")
                input_point_radius_metric = st.selectbox("Select point radius metric", ['TOTAL_SALES_USD', 'CUSTOMER_LOYALTY_VISITOR_COUNT'])
                create_point_map(df_location_farther_from_top_point, input_point_radius_metric, input_enable_lines)
                st.table(data=df_location_farther_from_top_point)

            with col2:
                st.subheader("Top H3 Hexagons based on Sales")
                input_color_division = st.number_input("Enter color division", min_value=3, max_value=6, value=3)
                input_elevation_metric = st.selectbox("Select elevation metric", ['TOTAL_SALES_USD', 'CUSTOMER_LOYALTY_VISITOR_COUNT'])
                create_hexagon_map(df_hex_top_locations, input_elevation_metric, input_color_division)
                st.table(data=df_hex_top_locations)


orders_reference_associations = permission.get_reference_associations("order_table")
if len(orders_reference_associations) == 0:
    permission.request_reference("order_table")
    exit(0)


st.title("Geospatial Insights on Sales Performance by Location")
st.write("This application provides powerful geospatial visualizations using Snowflake built-in geospatial functions and streamlit. Choose Country and City of interest and select required parameters to customize visuals.")
st.divider()
orders_table = "reference('order_table')"
load_app(orders_table)
