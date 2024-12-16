import urllib.request
import json
import pandas as pd
import requests_cache
import requests
from retry_requests import retry
import sqlite3
from datetime import datetime, timedelta

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

# Fetch the most recent date for each table
def get_last_date(table_name):
    cursor.execute(f"SELECT MAX(date) FROM {table_name}")
    result = cursor.fetchone()[0]
    return datetime.strptime(result, '%Y-%m-%d') if result else None

# Fetch up to 25 items from the weather API
def fetch_weather_data():
    last_date = get_last_date('daily_temperatures')
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"

    try:
        response = urllib.request.urlopen(url)
        jsonData = json.load(response)
        daily_data = jsonData.get("days", [])
        weather_data = pd.DataFrame([
            {"date": day["datetime"], "high_temperature": day["tempmax"]}
            for day in daily_data
        ])
        weather_data.to_sql('daily_temperatures', conn, if_exists='append', index=False)
        print(f"Stored weather data from {start_date} to {end_date}.")
        
        # Print the contents of the 'daily_temperatures' table
        cursor.execute("SELECT * FROM daily_temperatures")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error fetching weather data: {e}")

# Fetch up to 25 items from the UV index API
def fetch_uv_data():
    last_date = get_last_date('daily_uv_index')
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 44.3148,
        "longitude": -85.6024,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d'),
        "daily": "uv_index_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }

    try:
        response = requests.get(url, params=params).json()
        daily_data = response.get("daily", {})
        uv_data = pd.DataFrame({
            "date": daily_data["time"],
            "high_uv": daily_data["uv_index_max"]
        })
        uv_data.to_sql('daily_uv_index', conn, if_exists='append', index=False)
        print(f"Stored UV data from {start_date} to {end_date}.")
        
        # Print the contents of the 'daily_uv_index' table
        cursor.execute("SELECT * FROM daily_uv_index")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"Error fetching UV data: {e}")

# Fetch up to 25 items from the Google Trends API
def fetch_google_trends_data():
    last_date = get_last_date('google_searches')
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    api_key = "fd786e6ee27c1497261eaf57a23bf548c8772fb361d4b43f9745d57a7b9957a9"
    api_url = "https://serpapi.com/search.json"

    dates, hot_chocolate_values, lemonade_values = [], [], []

    current_date = start_date
    while current_date <= end_date:
        try:
            # Fetch hot chocolate trends
            params_hot_chocolate = {
                "engine": "google_trends",
                "q": "hot chocolate",
                "date": current_date.strftime('%Y-%m-%d'),
                "api_key": api_key
            }
            response_hot_chocolate = requests.get(api_url, params=params_hot_chocolate).json()
            hot_chocolate_data = response_hot_chocolate.get("interest_over_time", {}).get("timeline_data", [])
            hot_chocolate_value = sum(entry["values"][0]["value"] for entry in hot_chocolate_data) if hot_chocolate_data else 0

            # Fetch lemonade trends
            params_lemonade = {
                "engine": "google_trends",
                "q": "lemonade",
                "date": current_date.strftime('%Y-%m-%d'),
                "api_key": api_key
            }
            response_lemonade = requests.get(api_url, params=params_lemonade).json()
            lemonade_data = response_lemonade.get("interest_over_time", {}).get("timeline_data", [])
            lemonade_value = sum(entry["values"][0]["value"] for entry in lemonade_data) if lemonade_data else 0

            # Append results
            dates.append(current_date.strftime('%Y-%m-%d'))
            hot_chocolate_values.append(hot_chocolate_value)
            lemonade_values.append(lemonade_value)
        except Exception as e:
            print(f"Error fetching data for {current_date.strftime('%Y-%m-%d')}: {e}")
            dates.append(current_date.strftime('%Y-%m-%d'))
            hot_chocolate_values.append(0)
            lemonade_values.append(0)

        current_date += timedelta(days=1)

    trends_df = pd.DataFrame({
        "date": dates,
        "hot_chocolate_searches": hot_chocolate_values,
        "lemonade_searches": lemonade_values
    })
    trends_df.to_sql('google_searches', conn, if_exists='append', index=False)
    print(f"Stored Google Trends data from {start_date} to {end_date}.")

    # Print the contents of the 'google_searches' table
    cursor.execute("SELECT * FROM google_searches")
    rows = cursor.fetchall()
    for row in rows:
        print(row)


# Fetch data from APIs
fetch_weather_data()
fetch_uv_data()
fetch_google_trends_data()

# Close the database connection
conn.close()
