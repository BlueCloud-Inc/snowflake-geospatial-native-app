import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import count_distinct,col,sum
import snowflake.permissions as permission
from sys import exit

st.set_page_config(layout="wide")
session = get_active_session()

def load_app(orders_table):
    with st.spinner("Loading sale locations insights, please wait...."):
        df_dummy=session.sql(f"CREATE OR REPLACE VIEW orders_v COMMENT = 'Tasty Bytes Order Detail View' AS SELECT DATE(o.order_ts) AS date,o.* ,cpg.* EXCLUDE (location_id, region, phone_number, country) FROM {orders_table} o JOIN frostbyte_tb_safegraph_s cpg ON o.location_id = cpg.location_id;").collect()
        df_location_farther_from_top_point = session.sql(f"WITH _CENTER_POINT AS(WITH _top_10_locations AS(SELECT TOP 10 o.location_id, ST_MAKEPOINT(o.longitude, o.latitude) AS geo_point, SUM(o.price) AS total_sales_usd FROM ORDERS_V o WHERE primary_city = 'Paris' GROUP BY o.location_id, o.latitude, o.longitude ORDER BY total_sales_usd DESC) SELECT ST_COLLECT(tl.geo_point) AS collect_points, ST_CENTROID(collect_points) AS geometric_center_point FROM _top_10_locations tl), _paris_locations AS(SELECT DISTINCT location_id,location_name,ST_MAKEPOINT(longitude, latitude) AS geo_point,latitude,longitude FROM ORDERS_V WHERE primary_city = 'Paris') SELECT TOP 50 location_id,location_name,ROUND(ST_DISTANCE(geo_point, TO_GEOGRAPHY(_CENTER_POINT.geometric_center_point))/1000,2) AS kilometer_from_top_selling_center,longitude, latitude,_CENTER_POINT.geometric_center_point FROM _paris_locations,_CENTER_POINT ORDER BY kilometer_from_top_selling_center DESC").to_pandas()

        df_hex_top_locations = session.sql(f"WITH _top_50_locations AS(SELECT TOP 50 location_id,ARRAY_SIZE(ARRAY_UNIQUE_AGG(customer_id)) AS customer_loyalty_visitor_count,H3_LATLNG_TO_CELL(latitude, longitude, 7) AS h3_integer_resolution_6,H3_LATLNG_TO_CELL_STRING(latitude, longitude, 7) AS h3_hex_resolution_6,SUM(price) AS total_sales_usd FROM orders_v WHERE primary_city = 'Paris' GROUP BY ALL ORDER BY total_sales_usd DESC) SELECT h3_hex_resolution_6,COUNT(DISTINCT location_id) AS number_of_top_50_locations, SUM(customer_loyalty_visitor_count) AS customer_loyalty_visitor_count,SUM(total_sales_usd) AS total_sales_usd FROM _top_50_locations GROUP BY ALL ORDER BY total_sales_usd DESC").to_pandas()
        
        with st.container():
            col1,col2 = st.columns(2,gap='small')
            with col1:
                # Display Lead Time Status chart
                st.subheader("distance from top selling locations")
                st.table(data=df_location_farther_from_top_point)
        
            def color_lead_time(val):
                return f'background-color: rgb(249,158,54)'
        
            with col2:
                # Underlying Data
                st.subheader("top hexagons with sales and locations")
                st.table(data=df_hex_top_locations)
        
        

orders_reference_associations = permission.get_reference_associations("order_table")
if len(orders_reference_associations) == 0:
    permission.request_reference("order_table")
    exit(0)


st.title("Sale location insights")
orders_table = "reference('order_table')"
load_app(orders_table)
