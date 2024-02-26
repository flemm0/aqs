# Air Quality Data Pipeline

This project is a data pipeline designed to retrieve hourly measurements of air pollutants from the AirNow API provided by the US Environmental Protection Agency, store the data in Amazon S3, and then load it into an Amazon Redshift data warehouse for further analysis. The data warehouse provides easy access to up-to-date air quality data across the United States. This enables users to gain immediate insights into current air pollutant levels in various regions across the world. The data model is structured to contain hourly measurements of four key air pollutants: ozone, PM 10, PM 2.5, and NO2. The structured data model facilitates historical analysis of air quality trends over time. By storing hourly measurements, users can analyze patterns, seasonal variations, and long-term trends in air pollutant concentrations.

Access to comprehensive air quality data empowers policymakers, researchers, and environmental advocates to make data-driven decisions. Whether it's assessing the effectiveness of pollution control measures, identifying areas of concern, or evaluating the impact of environmental policies, this data pipeline supports informed decision-making processes. The pipeline architecture is designed for scalability and automation, allowing for seamless processing of large volumes of data over time. With scheduled data retrieval, storage, and loading processes, users can rely on consistent and up-to-date air quality data without manual intervention.

# Data Pipeline
![Data Pipeline](images/data_pipeline_diagram.png)

# Data Model
![Data Model](images/AirNow_AQObs_Data_Model.png)