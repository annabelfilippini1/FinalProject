import sqlite3
import requests
import datetime
import os
import pandas as pd

# API parameters
api_key = "C8P3R2GCZPUCQYRYDLDCPEZAZ"
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/48104/2023-01-01/2023-12-31"
params = {"unitGroup": "us", "key": api_key, "include": "days"}

# Fetch weather data
response = requests.get(url, params=params)
data = response.json()["days"]

# Create SQLite database and table
conn = sqlite3.connect("weather_data.db")
conn.execute("CREATE TABLE IF NOT EXISTS Weather (Date INTEGER PRIMARY KEY, HighTemperature FLOAT)")

# Insert data
rows = [(int(datetime.strptime(day["datetime"], "%Y-%m-%d").strftime("%Y%m%d")), day["tempmax"]) for day in data]
conn.executemany("INSERT OR IGNORE INTO Weather (Date, HighTemperature) VALUES (?, ?)", rows)

# Save and close
conn.commit()
conn.close()










# Ensure 'data' directory exists
os.makedirs('data', exist_ok=True)

# Setup Database
def setup_database():
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    # TODO: creates 3 tables. uv, temp, google trends

    connection.commit()
    connection.close()
    print("Database and tables created successfully!")


# Fetch Weather Data for Michigan
def fetch_weather_data(batch_size=25):
    API_KEY = '0cc603418077cee513623172b9a1d8c2'
    lat = 44.31  # Michigan latitude
    lon = -85.6  # Michigan longitude
    base_url = "https://history.openweathermap.org/data/2.5/history/city"

    # Check how many entries are already in the database
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    # TODO

    # first fetch data and get a pandas df, then insert the df into SQL


    connection.commit()
    connection.close()
    print(f"Stored {batch_size} weather data entries successfully!")


# Fetch Google Trends Data
def fetch_google_trends(keyword, batch_size=25):
    API_KEY = '4c668174a9439e8e26f6c233750fa3d50d8de4bd2d28fb757fb14b9eec28d111'
    base_url = "https://serpapi.com/search"
    params = {
        'engine': 'google_trends',
        'q': keyword,
        'api_key': API_KEY
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(f"Error: Unable to fetch Google Trends data for {keyword}. HTTP Status Code: {response.status_code}")
        return

    data = response.json()
    print(f"Data fetched for keyword '{keyword}': {data}")  # Debug print

    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    if 'interest_over_time' in data:
        entries_stored = 0
        for trend in data['interest_over_time']:
            print(f"Processing trend data: {trend}")  # Debug print
            if entries_stored >= batch_size:
                break
            try:
                cursor.execute("INSERT OR IGNORE INTO keywords (keyword) VALUES (?)", (keyword,))
                cursor.execute("SELECT id FROM keywords WHERE keyword = ?", (keyword,))
                keyword_id = cursor.fetchone()[0]

                # Insert search trends data with the keyword_id reference
                cursor.execute('''
                INSERT INTO search_trends (keyword_id, platform, volume, date)
                VALUES (?, ?, ?, ?)
                ''', (keyword_id, 'Google', trend['value'], trend['time']))
            except Exception as e:
                print(f"Error inserting trend data into the database: {e}")

    connection.commit()
    connection.close()
    print(f"Stored {entries_stored} entries for Google Trends keyword '{keyword}'!")


def print_database_data():
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    print("Weather Data:")
    cursor.execute("SELECT * FROM weather_data")
    for row in cursor.fetchall():
        print(row)

    print("\nSearch Trends Data:")
    cursor.execute("SELECT * FROM search_trends")
    for row in cursor.fetchall():
        print(row)

    connection.close()


# Main execution
if __name__ == "__main__":
    setup_database()
    fetch_weather_data()
    fetch_google_trends('lemonade')
    fetch_google_trends('hot chocolate')
    print_database_data()

