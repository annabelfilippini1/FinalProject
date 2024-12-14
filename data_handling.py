import urllib.request
import json
import pandas as pd
import openmeteo_requests
import requests_cache
import requests
from retry_requests import retry


# Define the API URL
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/2023-01-01/2023-12-31?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"

try:
    # Make the API request
    response = urllib.request.urlopen(url)
    
    # Parse the JSON response
    jsonData = json.load(response)
    
    # Extract daily data
    daily_data = jsonData.get("days", [])
    
    # Create a DataFrame with the desired fields
    weather_data = pd.DataFrame([
        {"date": day["datetime"], "high_temperature": day["tempmax"]}
        for day in daily_data
    ])
    
    # Print every day and its high temperature
    print(weather_data.to_string(index=False))  # Print all rows without an index column
    
except urllib.error.HTTPError as e:
    error_info = e.read().decode()
    print("HTTP Error:", e.code, error_info)
except urllib.error.URLError as e:
    print("URL Error:", e.reason)
except Exception as e:
    print("An error occurred:", e)

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
params = {
    "latitude": 44.3148,
    "longitude": 85.6024,
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "daily": "uv_index_max",
    "temperature_unit": "fahrenheit",
    "timezone": "America/New_York"
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Process daily data. The order of variables needs to be the same as requested.
daily = response.Daily()
daily_uv_index_max = daily.Variables(0).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
    start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
    end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
    freq = pd.Timedelta(seconds = daily.Interval()),
    inclusive = "left"
)}
daily_data["uv_index_max"] = daily_uv_index_max

daily_dataframe = pd.DataFrame(data = daily_data)
print(daily_dataframe)

# GOOGLE TRENDS

import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# SQLite Database Connection
api_key = "2cf86170782ffcf4dcffb29f53d499162ec447c38128f29cd9c5cd4d9825d9ee"
api_url = "https://serpapi.com/search.json"

# Define start and end dates
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# For testing, you can limit to a smaller range:
# start_date = datetime(2023, 1, 1)
# end_date = datetime(2023, 1, 7)

# Initialize lists for storing data
dates = []
hot_chocolate_values = []
lemonade_values = []

# Iterate over each day in the year
current_date = start_date
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    date_range = f"{current_date.strftime('%Y-%m-%d')} {next_date.strftime('%Y-%m-%d')}"

    try:
        # Get data for "hot chocolate"
        params_hot_chocolate = {
            "engine": "google_trends",
            "q": "hot chocolate",
            "date": date_range,
            "api_key": api_key
        }
        response_hot_chocolate = requests.get(api_url, params=params_hot_chocolate)
        response_hot_chocolate.raise_for_status()
        hot_chocolate_data = response_hot_chocolate.json().get("interest_over_time", {}).get("timeline_data", [])
        hot_chocolate_value = sum(entry["values"][0]["extracted_value"] for entry in hot_chocolate_data) if hot_chocolate_data else 0

        # Get data for "lemonade"
        params_lemonade = {
            "engine": "google_trends",
            "q": "lemonade",
            "date": date_range,
            "api_key": api_key
        }
        response_lemonade = requests.get(api_url, params=params_lemonade)
        response_lemonade.raise_for_status()
        lemonade_data = response_lemonade.json().get("interest_over_time", {}).get("timeline_data", [])
        lemonade_value = sum(entry["values"][0]["extracted_value"] for entry in lemonade_data) if lemonade_data else 0

        # Append to lists
        dates.append(current_date.strftime('%Y-%m-%d'))
        hot_chocolate_values.append(hot_chocolate_value)
        lemonade_values.append(lemonade_value)

        # Print progress for each date
        # After the loop ends, print all processed data
        for date, hot_chocolate, lemonade in zip(dates, hot_chocolate_values, lemonade_values):
            print(f"{date} - Hot Chocolate: {hot_chocolate}, Lemonade: {lemonade}")


    except Exception as e:
        print(f"Error on {current_date.strftime('%Y-%m-%d')}: {e}")
        dates.append(current_date.strftime('%Y-%m-%d'))
        hot_chocolate_values.append(0)
        lemonade_values.append(0)

    # Move to the next day
    current_date = next_date
    time.sleep(2)  # Delay to prevent rate limiting

# Create a DataFrame with the results
trends_df = pd.DataFrame({
    "date": dates,
    "hot_chocolate": hot_chocolate_values,
    "lemonade": lemonade_values
})

# Print the first few rows of the trends data to verify it worked
print("Trends Data Sample:")
print(trends_df.head())

# Save the DataFrame to the SQLite database
trends_df.to_sql("trends_data", conn, if_exists="replace", index=False)

# Confirm successful creation
print("Third table 'trends_data' created successfully.")

# Close the database connection
conn.close()
