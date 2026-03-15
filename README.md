# cardinals-ticket-weather-pipeline
Data pipeline analyzing ticket engagement and weather forecasts for St. Louis Cardinals games
____________________________________________________________________________
# Project Summary
-This project demonstrates how a data pipeline can integrate ticket activity data with weather forecasts to analyze how fan engagement changes as game day approaches.
-The pipeline ingests simulated ticket activity data, retrieves weather forecast data using a public API, and transforms the combined dataset into a table that allows exploration of how weather forecasts influence ticket demand.
-The final dataset enables analysts to study how evolving weather forecasts correlate with ticket page engagement in the days leading up to a game.
____________________________________________________________________________
# Architecture
-The pipeline follows a ETL (Extract, Transform, Load) architecture that pulls raw data from multiple sources, cleans, and transform it. The workflow is as follows: 
Ticket Activity Dataset --> Python Ingestion script --> SQLite Database includes Weather Forecast API --> Transformation Layer --> Final analytics table called fact_tickey_interest_weather

# Pipeline Components 
-Ticket activity data is loaded into the database. 
-Weather forecasts are retrived using Open-Meteo API 
-Tickey and weather datases are joined and transformed into a final table.
- A single python script runs the full pipeline. The pipeline scripts are as structured as: pipeline --> ingest_tickey_data.py, pull_weather_data.py, transform_data.py, and run_pipeline.py
  
# Pipeline Structure 
-The pipeline was structured using Visual Studio Code (VS code) python extension for efficiency. 
-The first script ingest_ticket_data.py read the CSV file, converted date columns into datetime format, calculated days_before_gate and loaded the results into SQLite as tickey_activity from the pandas library (i.e. pd.to_datetime(), df.to_sql())
-The second script pulled weather forecast data from Open-Meteo API by making an API request for St. Louis weather and retrieved forecast date, temperature, precipitation probability, and weather code and stored the ouput as weather_forecast.
-The third script transform_data.py reads in the ticket_activity and weather_forecast joined weather data to ticket snaphots and then translated weather codes and created the final table (fact_ticket_interest_weather)
-Finally, run_pipeline.py ran the three main pipleline steps in sequence by importing the functions from the other scripts. This adds the automation instead of running each step, the whole workflow could be executed with one command.
____________________________________________________________________________
# Modeling Decisions
- The final dataset is modeled for game_date + snapshot_date because each row represents a snapshot of ticket engagement for a specific game observed on a specific day before the game. This will allow analyst to measure how engagement changes over time as the game approaches.
- A key feature was created: days_before_game = game_date - snapshot_date to analyze engagement trends as game day approaches.
- The final analytics table is fact_ticket_interest_weather which include key fields such as: snapshot_date, game_date, days_before_game, unique_page_clicks, unique_tickets_sold, forecast_temp, forecast_precip_prob, forecast_condition. 
____________________________________________________________________________
# Assumptions
- The provided dataset already included game_date which is used as the schedule reference
- Weather data was retrieved using a public API for St. Louis
- Forecast represents the conditions available at the time the pipeline runs.
____________________________________________________________________________
# Improvements With More Time 
- Add automated checks for missing data, schema changes, and unexpected values.
- Use Apache Airflow to schedule pipeline runs and monitor failures.
- Store datasets in BigQuery to support larger datasets and concurrent analyst queries.
