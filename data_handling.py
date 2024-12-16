import urllib.request
import json
import pandas as pd
import sqlite3
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()

# Create database schema
# Ensure tables exist without resetting data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_summary (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location TEXT,
        start_date TEXT,
        end_date TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_temperatures (
        weather_id INTEGER,
        date TEXT PRIMARY KEY,
        high_temperature REAL,
        FOREIGN KEY(weather_id) REFERENCES weather_summary(weather_id)
    )
''')
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
conn.commit()


cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_summary (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        location TEXT,
        start_date TEXT,
        end_date TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_temperatures (
        weather_id INTEGER,
        date TEXT PRIMARY KEY,
        high_temperature REAL,
        FOREIGN KEY(weather_id) REFERENCES weather_summary(weather_id)
    )
''')
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
conn.commit()

# Function to check existing data
def get_last_date(table_name, cursor):
    cursor.execute(f"SELECT MAX(date) FROM {table_name}")
    result = cursor.fetchone()[0]
    return datetime.strptime(result, '%Y-%m-%d') if result else None


# Function to insert weather summary
def insert_weather_summary(location, start_date, end_date, cursor):
    cursor.execute('''
        INSERT INTO weather_summary (location, start_date, end_date)
        VALUES (?, ?, ?)
    ''', (location, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
    conn.commit()
    return cursor.lastrowid

# Fetch weather data
def fetch_weather_data(conn, cursor):
    last_date = get_last_date('daily_temperatures', cursor)
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    weather_id = insert_weather_summary("Michigan", start_date, end_date, cursor)

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

# Fetch UV index data
def fetch_uv_data(conn, cursor):
    last_date = get_last_date('daily_uv_index', cursor)
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    weather_id = insert_weather_summary("Michigan", start_date, end_date, cursor)

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

# Fetch Google Trends data
def fetch_google_trends_data(conn, cursor):
    last_date = get_last_date('google_searches', cursor)
    start_date = last_date + timedelta(days=1) if last_date else datetime(2023, 1, 1)
    end_date = start_date + timedelta(days=24)

    api_key = "YOUR_API_KEY"
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
            hot_chocolate_value = sum(entry.get("value", 0) for entry in response_hot_chocolate.get("timeline_data", []))

            # Fetch lemonade trends
            params_lemonade = {
                "engine": "google_trends",
                "q": "lemonade",
                "date": current_date.strftime('%Y-%m-%d'),
                "api_key": api_key
            }
            response_lemonade = requests.get(api_url, params=params_lemonade).json()
            lemonade_value = sum(entry.get("value", 0) for entry in response_lemonade.get("timeline_data", []))

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

# Ensure minimum records in the database
# Ensure minimum records in the database
def ensure_minimum_records(cursor):
    cursor.execute("SELECT COUNT(*) FROM daily_temperatures")
    record_count = cursor.fetchone()[0]
    print(f"Total records in 'daily_temperatures': {record_count}")
    if record_count < 100:
        print("Run the script again to gather more data.")
    else:
        print("Sufficient data has been collected.")

if __name__ == "__main__":
    # Clear all existing records to restart from batch 1
    cursor.execute('DELETE FROM daily_temperatures')
    cursor.execute('DELETE FROM daily_uv_index')
    cursor.execute('DELETE FROM google_searches')
    cursor.execute('DELETE FROM weather_summary')
    conn.commit()
    print("All tables have been cleared. Starting fresh.")


    # Fetch data in batches
    batch_count = 0
    while True:
        print(f"Starting batch {batch_count + 1}...")
        ensure_minimum_records(cursor)

        # Fetch and store data
        fetch_weather_data(conn, cursor)
        fetch_uv_data(conn, cursor)
        fetch_google_trends_data(conn, cursor)

        batch_count += 1
        print(f"Batch {batch_count} fetched and stored.")

        # Check if 100+ records have been collected
        cursor.execute("SELECT COUNT(*) FROM daily_temperatures")
        record_count = cursor.fetchone()[0]
        if record_count >= 100:
            print(f"100+ records collected after {batch_count} batches. Stopping.")
            break

    

    # Data analysis and visualization
    temp_data = pd.read_sql('SELECT * FROM daily_temperatures', conn)
    uv_data = pd.read_sql('SELECT * FROM daily_uv_index', conn)
    search_data = pd.read_sql('SELECT * FROM google_searches', conn)

    # Convert dates to datetime format
    temp_data['date'] = pd.to_datetime(temp_data['date'])
    uv_data['date'] = pd.to_datetime(uv_data['date'])
    search_data['date'] = pd.to_datetime(search_data['date'])

    # Merge data
    merged_data = temp_data.merge(uv_data, on='date').merge(search_data, on='date')
    merged_data = merged_data.sort_values(by='date')

    # Calculations
    avg_temp = merged_data['high_temperature'].mean()
    print(f"Average Temperature: {avg_temp:.2f} °F")

    max_uv = merged_data['high_uv'].max()
    max_uv_date = merged_data.loc[merged_data['high_uv'].idxmax(), 'date']
    print(f"Maximum UV Index: {max_uv} on {max_uv_date}")

    # --- Graph 1: Temperature and UV Index ---
    plt.figure(figsize=(12, 6))
    plt.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
    plt.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
    plt.title('Daily Trends: Temperature and UV Index (2023)')
    plt.xlabel('Date')
    plt.ylabel('Values')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # --- Graph 2: Temperature, UV Index, and Lemonade Searches ---
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Temperature (°F)', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax2 = ax1.twinx()
    ax2.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
    ax2.set_ylabel('UV Index', color='tab:orange')
    ax2.tick_params(axis='y', labelcolor='tab:orange')
    ax3 = ax1.twinx()
    ax3.spines.right.set_position(("outward", 60))
    ax3.plot(merged_data['date'], merged_data['lemonade_searches'], label='Lemonade Searches', color='tab:green')
    ax3.set_ylabel('Lemonade Searches', color='tab:green')
    ax3.tick_params(axis='y', labelcolor='tab:green')
    plt.title('Daily Trends: Temperature, UV Index, and Lemonade Searches (2023)')
    plt.grid(True)
    fig.tight_layout()
    plt.show()

    # --- Graph 3: Temperature, UV Index, and Hot Chocolate Searches ---
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(merged_data['date'], merged_data['high_temperature'], label='Temperature (°F)', color='tab:blue')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Temperature (°F)', color='tab:blue')
    ax1.tick_params(axis='y', labelcolor='tab:blue')
    ax2 = ax1.twinx()
    ax2.plot(merged_data['date'], merged_data['high_uv'], label='UV Index', color='tab:orange')
    ax2.set_ylabel('UV Index', color='tab:orange')
    ax2.tick_params(axis='y', labelcolor='tab:orange')
    ax3 = ax1.twinx()
    ax3.spines.right.set_position(("outward", 60))
    ax3.plot(merged_data['date'], merged_data['hot_chocolate_searches'], label='Hot Chocolate Searches', color='tab:red')
    ax3.set_ylabel('Hot Chocolate Searches', color='tab:red')
    ax3.tick_params(axis='y', labelcolor='tab:red')
    plt.title('Daily Trends: Temperature, UV Index, and Hot Chocolate Searches (2023)')
    plt.grid(True)
    fig.tight_layout()
    plt.show()

    # Close the database connection
    conn.close()
