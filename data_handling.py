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

import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Define API key and endpoint
api_key = "2cf86170782ffcf4dcffb29f53d499162ec447c38128f29cd9c5cd4d9825d9ee"
api_url = "https://serpapi.com/search.json"

# Define start and end dates
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

# Initialize lists for storing data
dates = []
lemonade_values = []

# Iterate over each day in the year
current_date = start_date
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    date_range = f"{current_date.strftime('%Y-%m-%d')} {next_date.strftime('%Y-%m-%d')}"
    
    params = {
        "engine": "google_trends",
        "q": "lemonade",
        "date": date_range,
        "api_key": api_key
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        json_data = response.json()

        # Extract timeline data
        timeline_data = json_data.get("interest_over_time", {}).get("timeline_data", [])
        if timeline_data:
            for entry in timeline_data:
                dates.append(entry["date"])
                for value_entry in entry["values"]:
                    if value_entry["query"] == "lemonade":
                        lemonade_values.append(value_entry["extracted_value"])
        else:
            # No data for this date, append 0
            dates.append(current_date.strftime('%Y-%m-%d'))
            lemonade_values.append(0)

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred for {current_date.strftime('%Y-%m-%d')}: {http_err}")
        dates.append(current_date.strftime('%Y-%m-%d'))
        lemonade_values.append(0)
    except Exception as e:
        print(f"An error occurred for {current_date.strftime('%Y-%m-%d')}: {e}")
        dates.append(current_date.strftime('%Y-%m-%d'))
        lemonade_values.append(0)

    current_date = next_date
    time.sleep(2)  # Delay to prevent rate limiting

# Create DataFrame and save results
df = pd.DataFrame({"Date": dates, "Lemonade": lemonade_values})
print(df)
df.to_csv("lemonade_trends_2023.csv", index=False)
