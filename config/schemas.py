from polars import Utf8, Float64, Int64

monitor_json_schema = {
    'state_code': Utf8,
    'country_code': Utf8,
    'site_number': Utf8,
    'poc': Int64,
    'parameter_name': Utf8,
    'open_date': Utf8,
    'close_date': Utf8,
    'concurred_exclusions': Utf8,
    'dominant_source': Utf8,
    'measurement_scale': Utf8,
    'measurement_scale_def': Utf8,
    'monitoring_objective': Utf8,
    'last_method_description': Utf8,
    'last_method_begin_date': Utf8,
    'naaqs_primary_monitor': Utf8,
    'qa_primary_monitor': Utf8,
    'monitor_type': Utf8,
    'networks': Utf8,
    'monitoring_agency_code': Utf8,
    'monitoring_agency': Utf8,
    'si_id': Int64,
    'latitude': Float64,
    'longitude': Float64,
    'datum': Utf8,
    'lat_lon_accuracy': Float64,
    'elevation': Int64,
    'probe_height': Float64,
    'pl_probe_location': Utf8,
    'local_site_name': Utf8,
    'address': Utf8,
    'state_name': Utf8,
    'county_name': Utf8,
    'city_name': Utf8,
    'cbsa_code': Utf8,
    'cbsa_name': Utf8,
    'csa_code': Utf8,
    'csa_name': Utf8,
    'tribal_code': Utf8,
    'tribe_name': Utf8
}

sample_json_schema = {
    'state_code': Utf8,
    'county_code': Utf8,
    'site_number': Utf8,
    'parameter_code': Utf8,
    'poc': Int64,
    'latitude': Float64,
    'longitude': Float64,
    'datum': Utf8,
    'parameter': Utf8,
    'date_local': Utf8,
    'time_local': Utf8,
    'date_gmt': Utf8,
    'time_gmt': Utf8,
    'sample_measurement': Float64,
    'units_of_measure': Utf8,
    'units_of_measure_code': Utf8,
    'sample_duration': Utf8,
    'sample_duration_code': Utf8,
    'sample_frequency': Utf8,
    'detection_limit': Float64,
    'uncertainty': Utf8,
    'qualifier': Utf8,
    'method_type': Utf8,
    'method': Utf8,
    'method_code': Utf8,
    'state': Utf8,
    'county': Utf8,
    'date_of_last_change': Utf8,
    'cbsa_code': Utf8
}

hourly_data_file_schema = {
    'valid_date': Utf8,
    'valid_timme': Utf8,
    'aqsid': Utf8,
    'site_name': Utf8,
    'gmt_offset': Utf8,
    'parameter_name': Utf8,
    'reporting_units': Utf8,
    'value': Int64,
    'data_source': Utf8
}

hourly_aqobs_file_schema = {
    'AQSID': Utf8,
    'SiteName': Utf8,
    'Status': Utf8,
    'EPARegion': Utf8,
    'Latitude': Float64,
    'Longitude': Float64,
    'Elevation': Float64,
    'GMTOffset': Int64,
    'CountryCode': Utf8,
    'StateName': Utf8,
    'ValidDate': Utf8,
    'ValidTime': Utf8,
    'DataSource': Utf8,
    'ReportingArea_PipeDelimited': Utf8,
    'OZONE_AQI': Int64,
    'PM10_AQI': Int64,
    'PM25_AQI': Int64,
    'NO2_AQI': Int64,
    'Ozone_Measured': Int64,
    'PM10_Measured': Int64,
    'PM25_Measured': Int64,
    'NO2_Measured': Int64,
    'PM25': Float64,
    'PM25_Unit': Utf8,
    'OZONE': Float64,
    'OZONE_Unit': Utf8,
    'NO2': Float64,
    'NO2_Unit': Utf8,
    'CO': Float64,
    'CO_Unit': Utf8,
    'SO2': Float64,
    'SO2_Unit': Utf8,
    'PM10': Float64,
    'PM10_Unit': Utf8
}

reporting_area_locations_v2_schema = {
    'reporting_area': Utf8,
    'state_code': Utf8,
    'country_code': Utf8,
    'forecasts': Utf8,
    'action_day_name': Utf8,
    'latitude': Float64,
    'longitude': Float64,
    'gmt_offset': Int64,
    'daylight_savings': Utf8,
    'standard_time_zone_label': Utf8,
    'daylight_savings_time_zone_label': Utf8,
    'twc_code': Utf8,
    'usa_today': Utf8,
    'forecast_source': Utf8
}

monitoring_site_location_v2_schema = {
    'StationID': Utf8,
    'AQSID': Utf8,
    'FullAQSID': Utf8,
    'Parameter': Utf8,
    'MonitorType': Utf8,
    'SiteCode': Int64,
    'SiteName': Utf8,
    'Status': Utf8,
    'AgencyID': Utf8,
    'AgencyName': Utf8,
    'EPARegion': Utf8,
    'Latitude': Float64,
    'Longitude': Float64,
    'Elevation': Float64,
    'GMTOffset': Int64,
    'CountryFIPS': Utf8,
    'CBSA_ID': Utf8,
    'CBSA_Name': Utf8,
    'StateAQSCode': Int64,
    'StateAbbreviation': Utf8,
    'CountryAQSCode': Int64,
    'CountryName': Utf8
}