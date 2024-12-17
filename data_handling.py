import urllib.request
import json
import pandas as pd
import sqlite3
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import sys      


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

    api_key = "7f531efa217006f20c40e7e4adf06b55f3394e44df226c56294fce8d1e61a81a"
    api_url = "https://serpapi.com/search.json"

    dates, hot_chocolate_values, lemonade_values = [], [], []


    try:
        # Fetch hot chocolate trends
        date_range = f"{start_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        params_hot_chocolate = {
            "engine": "google_trends",
            "q": "hot chocolate,lemonade",
            "date": date_range,
            "api_key": api_key
        }
        response_hot_chocolate = requests.get(api_url, params=params_hot_chocolate).json()
        print(response_hot_chocolate)
        
        # hot_chocolate_value = sum(entry.get("value", 0) for entry in response_hot_chocolate.get("timeline_data", []))

        timeline_data = response_hot_chocolate['interest_over_time']['timeline_data']

        # Initialize empty lists
        dates = []
        hot_chocolate_values = []
        lemonade_values = []

        # Loop through the timeline data to extract values
        for data in timeline_data:
            # Convert date format from "Jan 1, 2023" to "%Y-%m-%d"
            original_date = data['date']
            formatted_date = datetime.strptime(original_date, '%b %d, %Y').strftime('%Y-%m-%d')
            dates.append(formatted_date)
            
            # Extract values for hot chocolate and lemonade
            for value in data['values']:
                if value['query'] == 'hot chocolate':
                    hot_chocolate_values.append(int(value['value']))
                elif value['query'] == 'lemonade':
                    lemonade_values.append(int(value['value']))


        # Append results
        dates.append(dates)
        hot_chocolate_values.append(hot_chocolate_values)
        lemonade_values.append(lemonade_values)
    except Exception as e:
        print(f"Error fetching data for {start_date.strftime('%Y-%m-%d')}: {e}")
        dates.append(start_date.strftime('%Y-%m-%d'))
        hot_chocolate_values.append(0)
        lemonade_values.append(0)


    trends_df = pd.DataFrame({
        "date": dates,
        "hot_chocolate_searches": hot_chocolate_values,
        "lemonade_searches": lemonade_values
    })
    trends_df['hot_chocolate_searches'] = pd.to_numeric(trends_df['hot_chocolate_searches'], errors='coerce')
    trends_df['lemonade_searches'] = pd.to_numeric(trends_df['lemonade_searches'], errors='coerce')

    # Drop rows with NaN values (if any)
    trends_df = trends_df.dropna()

    # Ensure the columns are integers (if they should be integers)
    trends_df['hot_chocolate_searches'] = trends_df['hot_chocolate_searches'].astype(int)
    trends_df['lemonade_searches'] = trends_df['lemonade_searches'].astype(int)

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
    batch_count_google = 0
    batch_count_temp = 0
    batch_count_uv = 0

    amount_of_days = 100

    # Ensure Google Searches table has 100+ records
    while True:
        print(f"Starting Google Searches batch {batch_count_google + 1}...")

        fetch_google_trends_data(conn, cursor)  # Fetch Google Trends data

        batch_count_google += 1
        cursor.execute("SELECT COUNT(*) FROM google_searches")
        google_search_count = cursor.fetchone()[0]
        print(f"Google Searches Record Count: {google_search_count}")

        if google_search_count >= amount_of_days:
            print(f"Google Searches table reached 100+ records after {batch_count_google} batches.")
            break

    # Ensure Daily Temperatures table has 100+ records
    while True:
        print(f"Starting Daily Temperatures batch {batch_count_temp + 1}...")

        fetch_weather_data(conn, cursor)  # Fetch Weather data

        batch_count_temp += 1
        cursor.execute("SELECT COUNT(*) FROM daily_temperatures")
        temperature_count = cursor.fetchone()[0]
        print(f"Daily Temperatures Record Count: {temperature_count}")

        if temperature_count >= amount_of_days:
            print(f"Daily Temperatures table reached 100+ records after {batch_count_temp} batches.")
            break

    # Ensure UV Data table has 100+ records
    while True:
        print(f"Starting UV Data batch {batch_count_uv + 1}...")

        fetch_uv_data(conn, cursor)  # Fetch UV data

        batch_count_uv += 1
        cursor.execute("SELECT COUNT(*) FROM daily_uv_index")
        uv_count = cursor.fetchone()[0]
        print(f"UV Data Record Count: {uv_count}")

        if uv_count >= amount_of_days:
            print(f"UV Data table reached 100+ records after {batch_count_uv} batches.")
            break

    print("All tables now have 100+ records. Process complete.")

    # Data analysis and visualization
    query = """
    SELECT 
        temp.date,
        temp.high_temperature,
        uv.high_uv,
        search.hot_chocolate_searches,
        search.lemonade_searches
    FROM 
        daily_temperatures AS temp
    JOIN 
        daily_uv_index AS uv
    ON 
        temp.date = uv.date
    JOIN 
        google_searches AS search
    ON 
        temp.date = search.date
    """

    # Execute the query
    merged_data = pd.read_sql(query, conn)

    # Convert the date column to datetime format if necessary
    merged_data['date'] = pd.to_datetime(merged_data['date'])

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
    plt.savefig("temp_uv_plot.png")

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
    plt.savefig("lemonade_search_plot.png")

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
    plt.savefig("hot_chocolate_search_plot.png")

    # Close the database connection
    conn.close()
