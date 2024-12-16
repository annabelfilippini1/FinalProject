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

cursor.execute('DROP TABLE IF EXISTS daily_temperatures')
cursor.execute('DROP TABLE IF EXISTS daily_uv_index')
cursor.execute('DROP TABLE IF EXISTS weather_summary')
cursor.execute('DROP TABLE IF EXISTS google_searches')
# Create the summary table: weather_summary
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_summary (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location TEXT,
        start_date TEXT,
        end_date TEXT
    )
''')

# Modify the daily_temperatures table to include weather_id
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_temperatures (
        weather_id INTEGER,
        date TEXT PRIMARY KEY,
        high_temperature REAL,
        FOREIGN KEY(weather_id) REFERENCES weather_summary(weather_id)
    )
''')

# Modify the daily_uv_index table to include weather_id
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_uv_index (
        weather_id INTEGER,
        date TEXT PRIMARY KEY,
        high_uv REAL,
        FOREIGN KEY(weather_id) REFERENCES weather_summary(weather_id)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS google_searches (
        date TEXT PRIMARY KEY,
        hot_chocolate_searches INTEGER,
        lemonade_searches INTEGER
    )
''')
# Commit schema changes
conn.commit()

def insert_weather_summary(location, start_date, end_date):
    cursor.execute('''
        INSERT INTO weather_summary (location, start_date, end_date)
        VALUES (?, ?, ?)
    ''', (location, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    conn.commit()
    return cursor.lastrowid  # Return the generated weather_id


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

    # Insert a summary record and get weather_id
    weather_id = insert_weather_summary("Michigan", start_date, end_date)

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"

    try:
        response = urllib.request.urlopen(url)
        jsonData = json.load(response)
        daily_data = jsonData.get("days", [])
        weather_data = pd.DataFrame([
            {"weather_id": weather_id, "date": day["datetime"], "high_temperature": day["tempmax"]}
            for day in daily_data
        ])
        weather_data.to_sql('daily_temperatures', conn, if_exists='append', index=False)
        print(f"Stored weather data from {start_date} to {end_date}.")
    except Exception as e:
        print(f"Error fetching weather data: {e}")


# Fetch up to 25 items from the UV index API
def fetch_uv_data():
    last_date = get_last_date('daily_uv_index')
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    # Insert a summary record and get weather_id
    weather_id = insert_weather_summary("Michigan", start_date, end_date)

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
            "weather_id": weather_id,
            "date": daily_data["time"],
            "high_uv": daily_data["uv_index_max"]
        })
        uv_data.to_sql('daily_uv_index', conn, if_exists='append', index=False)
        print(f"Stored UV data from {start_date} to {end_date}.")
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

temp_data = pd.read_sql('SELECT * FROM daily_temperatures', conn)
uv_data = pd.read_sql('SELECT * FROM daily_uv_index', conn)
search_data = pd.read_sql('SELECT * FROM google_searches', conn)

temp_data['date'] = pd.to_datetime(temp_data['date'])
uv_data['date'] = pd.to_datetime(uv_data['date'])
search_data['date'] = pd.to_datetime(search_data['date'])

# Merge Data
merged_data = temp_data.merge(uv_data, on='date').merge(search_data, on='date')

# Sort data by date
merged_data = merged_data.sort_values(by='date')


# --- Graph 1: Temperature and UV Index ---
plt.figure(figsize=(12, 6))
plt.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
plt.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')

# Primary y-axis: Temperature
fig, ax1 = plt.subplots(figsize=(12, 6))
ax1.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
ax1.set_xlabel('Date')
ax1.set_ylabel('Temperature (°F)', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')
# Secondary y-axis: UV Index
ax2 = ax1.twinx()
ax2.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
ax2.set_ylabel('UV Index', color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')
# Title, grid, and layout
plt.title('Daily Trends: Temperature and UV Index (2023)')
plt.xlabel('Date')
plt.ylabel('Values')
ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
plt.xticks(rotation=45, fontsize=8)
plt.gca().xaxis.set_major_locator(plt.MaxNLocator(12))
plt.legend()
fig.tight_layout()
plt.grid()
plt.tight_layout()
plt.show()

# --- Graph 2: Temperature, UV Index, and Lemonade Searches ---
plt.figure(figsize=(12, 6))
plt.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
plt.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
plt.plot(merged_data['date'], merged_data['lemonade_searches'], label='Lemonade Searches', color='tab:green')
fig, ax1 = plt.subplots(figsize=(12, 6))
# Primary y-axis: Temperature
ax1.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
ax1.set_xlabel('Date')
ax1.set_ylabel('Temperature (°F)', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')
# Secondary y-axis: UV Index
ax2 = ax1.twinx()
ax2.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
ax2.set_ylabel('UV Index', color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')
# Tertiary y-axis: Lemonade Searches
ax3 = ax1.twinx()
ax3.spines.right.set_position(("outward", 60))
ax3.plot(merged_data['date'], merged_data['lemonade_searches'], label='Lemonade Searches', color='tab:green')
ax3.set_ylabel('Lemonade Searches', color='tab:green')
ax3.tick_params(axis='y', labelcolor='tab:green')
# Title, grid, and layout
plt.title('Daily Trends: Temperature, UV Index, and Lemonade Searches (2023)')
plt.xlabel('Date')
plt.ylabel('Values')
ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
plt.xticks(rotation=45, fontsize=8)
plt.gca().xaxis.set_major_locator(plt.MaxNLocator(12))
plt.legend()
fig.tight_layout()
plt.grid()
plt.tight_layout()
plt.show()

# --- Graph 3: Temperature, UV Index, and Hot Chocolate Searches ---
plt.figure(figsize=(12, 6))
plt.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
plt.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
plt.plot(merged_data['date'], merged_data['hot_chocolate_searches'], label='Hot Chocolate Searches', color='tab:red')
fig, ax1 = plt.subplots(figsize=(12, 6))
# Primary y-axis: Temperature
ax1.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
ax1.set_xlabel('Date')
ax1.set_ylabel('Temperature (°F)', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')
# Secondary y-axis: UV Index
ax2 = ax1.twinx()
ax2.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
ax2.set_ylabel('UV Index', color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')
# Tertiary y-axis: Hot Chocolate Searches
ax3 = ax1.twinx()
ax3.spines.right.set_position(("outward", 60))
ax3.plot(merged_data['date'], merged_data['hot_chocolate_searches'], label='Hot Chocolate Searches', color='tab:red')
ax3.set_ylabel('Hot Chocolate Searches', color='tab:red')
ax3.tick_params(axis='y', labelcolor='tab:red')
# Title, grid, and layout
plt.title('Daily Trends: Temperature, UV Index, and Hot Chocolate Searches (2023)')
plt.xlabel('Date')
plt.ylabel('Values')
ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
plt.xticks(rotation=45, fontsize=8)
plt.gca().xaxis.set_major_locator(plt.MaxNLocator(12))
plt.legend()
fig.tight_layout()
plt.grid()
plt.tight_layout()
plt.show()
# Close the database connection
#conn.close()


