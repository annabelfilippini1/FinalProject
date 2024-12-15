import urllib.request
import json
import pandas as pd
import openmeteo_requests
import requests_cache
import requests
from retry_requests import retry
import sqlite3
import time
from datetime import datetime, timedelta
import matplotlib.pyplot as plt


# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Create the first table: daily temperatures
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_temperatures (
        date TEXT PRIMARY KEY,
        high_temperature REAL
    )
''')

# Create the second table: daily UV index
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_uv_index (
        date TEXT PRIMARY KEY,
        high_uv REAL
    )
''')

# Create the third table: Google searches for hot chocolate and lemonade
cursor.execute('''
    CREATE TABLE IF NOT EXISTS google_searches (
        date TEXT PRIMARY KEY,
        hot_chocolate_searches INTEGER,
        lemonade_searches INTEGER
    )
''')

# Commit all changes
conn.commit()

print("Three tables created successfully.")

# Define the API URL for VisualCrossing weather data
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/2023-01-01/2023-12-31?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"

try:
    # Fetch and process weather data
    response = urllib.request.urlopen(url)
    jsonData = json.load(response)
    daily_data = jsonData.get("days", [])

    weather_data = pd.DataFrame([
        {"date": day["datetime"], "high_temperature": day["tempmax"]}
        for day in daily_data
    ])
    print(weather_data.head())  # Display a sample of the data

    # Insert into SQLite database
    weather_data.to_sql('daily_temperatures', conn, if_exists='replace', index=False)

except urllib.error.HTTPError as e:
    print(f"HTTP Error: {e.code} - {e.read().decode()}")
except urllib.error.URLError as e:
    print(f"URL Error: {e.reason}")
except Exception as e:
    print(f"An error occurred: {e}")

# Setup Open-Meteo API client with cache and retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Open-Meteo API request
try:
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 44.3148,
        "longitude": -85.6024,
        "start_date": "2023-01-01",
        "end_date": "2023-12-31",
        "daily": "uv_index_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }
    response = openmeteo.weather_api(url, params=params)[0]  # Single response for simplicity

    daily = response.Daily()
    uv_index_max = daily.Variables(0).ValuesAsNumpy()
    dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")

    uv_data = pd.DataFrame({
        "date": dates.strftime('%Y-%m-%d'),
        "high_uv": uv_index_max
    })
    print(uv_data.head())  # Display a sample of the UV data

    # Insert into SQLite database
    uv_data.to_sql('daily_uv_index', conn, if_exists='replace', index=False)

except Exception as e:
    print(f"Error fetching UV index data: {e}")

# Fetch Google Trends data for "hot chocolate" and "lemonade"
api_key = "62844a59beea731fde941dfd82872072e6bc3f6e16fd71d800193ab39bd3f13e"
api_url = "https://serpapi.com/search.json"

start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)

dates, hot_chocolate_values, lemonade_values = [], [], []

current_date = start_date
while current_date <= end_date:
    next_date = current_date + timedelta(days=1)
    date_range = f"{current_date.strftime('%Y-%m-%d')} {next_date.strftime('%Y-%m-%d')}"

    try:
        # Hot Chocolate data
        params_hot_chocolate = {
            "engine": "google_trends",
            "q": "hot chocolate",
            "date": date_range,
            "api_key": api_key
        }
        response_hot_chocolate = requests.get(api_url, params=params_hot_chocolate).json()
        hot_chocolate_data = response_hot_chocolate.get("interest_over_time", {}).get("timeline_data", [])
        hot_chocolate_value = sum(entry["values"][0]["extracted_value"] for entry in hot_chocolate_data) if hot_chocolate_data else 0

        # Lemonade data
        params_lemonade = {
            "engine": "google_trends",
            "q": "lemonade",
            "date": date_range,
            "api_key": api_key
        }
        response_lemonade = requests.get(api_url, params=params_lemonade).json()
        lemonade_data = response_lemonade.get("interest_over_time", {}).get("timeline_data", [])
        lemonade_value = sum(entry["values"][0]["extracted_value"] for entry in lemonade_data) if lemonade_data else 0

        # Append results
        dates.append(current_date.strftime('%Y-%m-%d'))
        hot_chocolate_values.append(hot_chocolate_value)
        lemonade_values.append(lemonade_value)

    except Exception as e:
        print(f"Error on {current_date.strftime('%Y-%m-%d')}: {e}")
        dates.append(current_date.strftime('%Y-%m-%d'))
        hot_chocolate_values.append(0)
        lemonade_values.append(0)

    current_date = next_date
    time.sleep(2)

# Save Google Trends data
trends_df = pd.DataFrame({
    "date": dates,
    "hot_chocolate_searches": hot_chocolate_values,
    "lemonade_searches": lemonade_values
})
trends_df.to_sql('google_searches', conn, if_exists='replace', index=False)

print("Google Trends data saved successfully.")

#graphs!!
conn = sqlite3.connect('weather_data.db')

# Load the tables into DataFrames
temp_data = pd.read_sql('SELECT * FROM daily_temperatures', conn)
uv_data = pd.read_sql('SELECT * FROM daily_uv_index', conn)
search_data = pd.read_sql('SELECT * FROM google_searches', conn)

# Close the database connection
conn.close()

# Convert the date columns to datetime for merging
temp_data['date'] = pd.to_datetime(temp_data['date'])
uv_data['date'] = pd.to_datetime(uv_data['date'])
search_data['date'] = pd.to_datetime(search_data['date'])

# Merge all tables on the 'date' column
merged_data = temp_data.merge(uv_data, on='date').merge(search_data, on='date')

# Display the first few rows of the merged DataFrame
print(merged_data.head())


merged_data['month'] = pd.to_datetime(merged_data['date']).dt.to_period('M')

# Aggregate data by month
monthly_data = merged_data.groupby('month').mean()

# First graph: Temperature, UV, and Hot Chocolate trends
plt.figure(figsize=(12, 6))
plt.plot(monthly_data.index.astype(str), monthly_data['high_temperature'], label='Temperature (°F)')
plt.plot(monthly_data.index.astype(str), monthly_data['high_uv'], label='UV Index')
plt.plot(monthly_data.index.astype(str), monthly_data['hot_chocolate_searches'], label='Hot Chocolate Searches')
plt.title('Monthly Trends: Temperature, UV, and Hot Chocolate Searches (2023)')
plt.xlabel('Month')
plt.ylabel('Values')
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Second graph: Temperature, UV, and Lemonade trends
plt.figure(figsize=(12, 6))
plt.plot(monthly_data.index.astype(str), monthly_data['high_temperature'], label='Temperature (°F)')
plt.plot(monthly_data.index.astype(str), monthly_data['high_uv'], label='UV Index')
plt.plot(monthly_data.index.astype(str), monthly_data['lemonade_searches'], label='Lemonade Searches')
plt.title('Monthly Trends: Temperature, UV, and Lemonade Searches (2023)')
plt.xlabel('Month')
plt.ylabel('Values')
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
# Close the database connection
conn.close()
