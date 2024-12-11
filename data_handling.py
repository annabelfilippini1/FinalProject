
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

# Define the API key and endpoint
api_key = "4c668174a9439e8e26f6c233750fa3d50d8de4bd2d28fb757fb14b9eec28d111"
api_url = "https://serpapi.com/search.json?engine=google_trends&q=lemonade&data_type=RELATED_TOPICS&api_key=4c668174a9439e8e26f6c233750fa3d50d8de4bd2d28fb757fb14b9eec28d111"

# Define parameters for the API request
params = {
    "engine": "google_trends",
    "q": "Lemonade",
    "data_type": "RELATED_TOPICS",
    "api_key": api_key
}

try:
    # Make the API request
    response = requests.get(api_url, params=params)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

    # Parse the JSON response
    json_data = response.json()

    # Extract related topics
    related_topics = json_data.get("related_topics", [])
    print("Related Topics Raw Data:", related_topics)  # Debug: Inspect raw data

    # Create a structured DataFrame if data exists
    if related_topics:
        topics_df = pd.DataFrame(related_topics)
        print("Related Topics DataFrame:")
        print(topics_df.to_string(index=False))
    else:
        print("No related topics data found.")

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP error occurred: {http_err}")
except Exception as err:
    print(f"An error occurred: {err}")
