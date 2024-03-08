import streamlit as st
import pandas as pd
import plotly.express as px
import geopandas as gpd

import duckdb


# Setup

st.set_page_config(layout='wide')

try:
    snowflake_conn = st.connection('snowflake')
except:
    motherduck_token = st.secrets['MOTHERDUCK_TOKEN']
    md_conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}', read_only=True)






# Tabs

tab1, tab2 = st.tabs(['World Map', 'Date Trend'])

with tab1:

    query = """
        SELECT 
            full_date AS "full_date",
            latitude AS "latitude",
            longitude AS "longitude",
            site_name AS "site_name"
        FROM airnow_aqs.reporting.AQI_By_Monitoring_Site
        ORDER BY full_date DESC;
    """

    try:
        df = snowflake_conn.query(query)
    except:
        df = md_conn.query(query).df()
    
    selected_date = st.selectbox('Select Date', df['full_date'].unique())
    selected_parameter = st.selectbox('Select Parameter', ['NO2', 'PM10', 'PM25', 'OZONE'])

    filtered_df = df[df['full_date'] == selected_date]
    # Plot the world map
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_cities'))
    fig = px.choropleth_mapbox(world, geojson=world.geometry, locations=world.index,
                                color_continuous_scale="Viridis",
                                hover_name='name',
                                mapbox_style="carto-positron",
                                zoom=1, center={"lat": 0, "lon": 0})

    # Add points to the map
    fig.add_scattermapbox(
        lat=filtered_df['latitude'],
        lon=filtered_df['longitude'],
        mode='markers',
        marker=dict(
            # size=filtered_df[f'{selected_parameter}_AQI'],
            color=filtered_df[f'{selected_parameter}_AQI'.lower()],
            colorscale='RdBu',
            opacity=0.7,
            colorbar=dict(title=f'{selected_parameter} AQI')
        ),
        hovertext=filtered_df['site_name']
    )

    # Update layout
    fig.update_layout(
        title=f'{selected_parameter} AQI Measurements Overlayed on World Map for {selected_date}',
        mapbox=dict(
            style="carto-positron",
            zoom=1,
            center={"lat": 0, "lon": 0}
        ),
        height=1000
    )

    # Display the plot using Streamlit
    st.plotly_chart(
        fig,
        use_container_width=True,
        height=1000
    )


with tab2:

    query = """
        SELECT full_date, 'PM10' AS "pollutant_type", AVG(pm10_aqi) AS "avg_aqi"
        FROM reporting.aqi_by_monitoring_site
        GROUP BY 1

        UNION ALL

        SELECT full_date, 'PM2.5' AS "pollutant_type", AVG(pm25_aqi) AS "avg_aqi"
        FROM reporting.aqi_by_monitoring_site
        GROUP BY 1

        UNION ALL

        SELECT full_date, 'Ozone' AS "pollutant_type", AVG(ozone_aqi) AS "avg_aqi"
        FROM reporting.aqi_by_monitoring_site
        GROUP BY 1

        UNION ALL

        SELECT full_date, 'NO2' AS "pollutant_type", AVG(no2_aqi) AS "avg_aqi"
        FROM reporting.aqi_by_monitoring_site
        GROUP BY 1

        ORDER BY 1 DESC, 2;
    """
    try:
        df = snowflake_conn.query(query)
    except:
        df = md_conn.query(query).df()
        
    fig = px.line(
        df, 
        x='full_date', 
        y='avg_aqi', 
        title='AQI Trend', 
        color='pollutant_type', 
        line_group='pollutant_type'
    )
    
    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Average AQI',
        legend_title='Pollutant Type',
        height=1000
    )
    
    st.plotly_chart(
        fig,
        use_container_width=True,
        height=1000
    )
