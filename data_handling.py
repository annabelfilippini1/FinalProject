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


# --- Setup SQLite Database ---
def setup_database():
    conn = sqlite3.connect('weather_data.db')
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_temperatures (
            date TEXT PRIMARY KEY,
            high_temperature REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_uv_index (
            date TEXT PRIMARY KEY,
            high_uv REAL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS google_searches (
            date TEXT PRIMARY KEY,
            hot_chocolate_searches INTEGER,
            lemonade_searches INTEGER
        )
    ''')
    conn.commit()
    print("Three tables created successfully.")
    return conn


# --- Fetch Weather Data (VisualCrossing API) ---
def fetch_weather_data(conn):
    url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/2023-01-01/2023-12-31?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"
    try:
        response = urllib.request.urlopen(url)
        jsonData = json.load(response)
        daily_data = jsonData.get("days", [])

        # Process data
        weather_data = pd.DataFrame([
            {"date": day["datetime"], "high_temperature": day["tempmax"]}
            for day in daily_data
        ])
        print(weather_data.head())  # Display sample data

        # Save to database
        weather_data.to_sql('daily_temperatures', conn, if_exists='replace', index=False)
    except Exception as e:
        print(f"Error fetching weather data: {e}")


# --- Fetch UV Index Data (Open-Meteo API) ---
# --- Fetch UV Index Data ---
def fetch_uv_data(conn):
    try:
        # Open-Meteo API
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

        # Make API request
        response = requests.get(url, params=params).json()

        # Process response
        uv_index_max = response.get("daily", {}).get("uv_index_max", [])
        dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")

        # Create a DataFrame
        uv_data = pd.DataFrame({
            "date": dates.strftime('%Y-%m-%d'),
            "high_uv": uv_index_max
        })
        print(uv_data.head())  # Display sample data

        # Save to SQLite database
        uv_data.to_sql('daily_uv_index', conn, if_exists='replace', index=False)

    except Exception as e:
        print(f"Error fetching UV index data: {e}")

# --- Fetch Google Trends Data ---
def fetch_google_trends_data(conn):
    api_key = "62844a59beea731fde941dfd82872072e6bc3f6e16fd71d800193ab39bd3f13e"
    api_url = "https://serpapi.com/search.json"

    # Date range for Google Trends
    start_date = datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=9)  # Only process 10 days

    dates, hot_chocolate_values, lemonade_values = [], [], []
    current_date = start_date

    while current_date <= end_date:
        next_date = current_date + timedelta(days=1)
        date_range = f"{current_date.strftime('%Y-%m-%d')} {next_date.strftime('%Y-%m-%d')}"

        try:
            # Fetch "hot chocolate" data
            params_hot_chocolate = {
                "engine": "google_trends",
                "q": "hot chocolate",
                "date": date_range,
                "api_key": api_key
            }
            response_hot_chocolate = requests.get(api_url, params=params_hot_chocolate).json()
            hot_chocolate_data = response_hot_chocolate.get("interest_over_time", {}).get("timeline_data", [])
            hot_chocolate_value = sum(entry["values"][0]["extracted_value"] for entry in hot_chocolate_data) if hot_chocolate_data else 0

            # Fetch "lemonade" data
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

        # Increment date
        current_date = next_date
        time.sleep(2)  # Rate-limiting

    # Create DataFrame
    trends_df = pd.DataFrame({
        "date": dates,
        "hot_chocolate_searches": hot_chocolate_values,
        "lemonade_searches": lemonade_values
    })

    # Save to SQLite database
    trends_df.to_sql('google_searches', conn, if_exists='replace', index=False)

    # Display results
    print(trends_df.head(10))


# --- Main Script ---
def main():
    conn = setup_database()
    fetch_weather_data(conn)
    fetch_uv_data(conn)
    fetch_google_trends_data(conn)

    # Load and merge data
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
    ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
    plt.xticks(rotation=45, fontsize=8)
    fig.tight_layout()
    plt.grid()
    plt.show()

    # --- Graph 2: Temperature, UV Index, and Lemonade Searches ---
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
    ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
    plt.xticks(rotation=45, fontsize=8)
    fig.tight_layout()
    plt.grid()
    plt.show()

    # --- Graph 3: Temperature, UV Index, and Hot Chocolate Searches ---
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
    ax1.xaxis.set_major_locator(plt.MaxNLocator(12))
    plt.xticks(rotation=45, fontsize=8)
    fig.tight_layout()
    plt.grid()
    plt.show()


    conn.close()


# --- Ensure Proper Execution ---
if __name__ == "__main__":
    main()
