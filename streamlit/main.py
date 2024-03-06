import streamlit as st
import pandas as pd
import plotly.express as px
import geopandas as gpd




# Setup

st.set_page_config(layout='wide')


conn = st.connection('snowflake')

df = conn.query('SELECT * FROM AQI_By_Monitoring_Site ORDER BY full_date DESC;')

selected_date = st.sidebar.selectbox('Select Date', df['FULL_DATE'].unique())
selected_parameter = st.sidebar.selectbox('Select Parameter', ['NO2', 'PM10', 'PM25', 'OZONE'])




# Tabs

tab1, tab2 = st.tabs(['World Map', 'Date Trend'])

with tab1:
    
    filtered_df = df[df['FULL_DATE'] == selected_date]
    # Plot the world map
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_cities'))
    fig = px.choropleth_mapbox(world, geojson=world.geometry, locations=world.index,
                                color_continuous_scale="Viridis",
                                hover_name='name',
                                mapbox_style="carto-positron",
                                zoom=1, center={"lat": 0, "lon": 0})

    # Add points to the map
    fig.add_scattermapbox(
        lat=filtered_df['LATITUDE'],
        lon=filtered_df['LONGITUDE'],
        mode='markers',
        marker=dict(
            # size=filtered_df[f'{selected_parameter}_AQI'],
            color=filtered_df[f'{selected_parameter}_AQI'],
            colorscale='RdBu',
            opacity=0.7,
            colorbar=dict(title=f'{selected_parameter} AQI')
        ),
        hovertext=filtered_df['SITE_NAME']
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


# with tab2:

#     fig = px.line(df, x='FULL_DATE', y=f'{selected_parameter}_AQI', title=f'{selected_parameter} AQI Trend',
#                 # color=f'{selected_parameter}_AQI', line_group=f'{selected_parameter}_AQI',
#                 labels={f'{selected_parameter}_AQI': 'Value', 'FULL_DATE': 'Date'})
    
#     fig.update_layout(
#         xaxis_title='FULL_DATE',
#         yaxis_title=f'{selected_parameter} AQI',
#         legend_title=f'{selected_parameter} AQI',
#         height=1000
#     )
    
#     st.plotly_chart(
#         fig,
#         use_container_width=True,
#         height=1000
#     )
