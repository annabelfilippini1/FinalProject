import sqlite3
import requests
import datetime
import os
import pandas as pd

# Ensure 'data' directory exists
os.makedirs('data', exist_ok=True)

# Setup Database
def setup_database():
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        average_temp REAL NOT NULL,
        temperature_range TEXT NOT NULL
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT NOT NULL,
        platform TEXT NOT NULL,
        volume INTEGER NOT NULL,
        date TEXT NOT NULL
    )
    ''')
    connection.commit()
    connection.close()
    print("Database and tables created successfully!")

# Fetch Weather Data for Michigan
def fetch_weather_data(batch_size=25):
    API_KEY = '0cc603418077cee513623172b9a1d8c2'
    lat = 44.31  # Michigan latitude
    lon = -85.6  # Michigan longitude
    base_url = "http://api.openweathermap.org/data/2.5/onecall/timemachine"

    # Check how many entries are already in the database
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_data")
    existing_count = cursor.fetchone()[0]

    # Generate timestamps for the next batch of days
    start_date = datetime.date(2018, 1, 1)
    current_date = start_date + datetime.timedelta(days=existing_count)
    end_date = current_date + datetime.timedelta(days=batch_size)

    while current_date < end_date:
        timestamp = int(datetime.datetime.combine(current_date, datetime.datetime.min.time()).timestamp())
        params = {
            'lat': lat,
            'lon': lon,
            'dt': timestamp,
            'appid': API_KEY,
            'units': 'imperial'
        }

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"Error fetching data for {current_date}: {response.status_code} - {response.text}")
            current_date += datetime.timedelta(days=1)
            continue

        try:
            data = response.json()
            print(f"Response for {current_date}:\n{data}")

            if 'current' in data:
                current = data['current']
                avg_temp = current['temp']
                temperature_range = f"{avg_temp} - {avg_temp}"  # Simplified due to API limitations

                cursor.execute('''
                INSERT INTO weather_data (date, average_temp, temperature_range)
                VALUES (?, ?, ?)
                ''', (current_date.isoformat(), avg_temp, temperature_range))
            else:
                print(f"'current' data missing for {current_date}")
        except Exception as e:
            print(f"Error processing data for {current_date}: {e}")

        current_date += datetime.timedelta(days=1)

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
    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    if 'interest_over_time' in data:
        entries_stored = 0
        for trend in data['interest_over_time']:
            if entries_stored >= batch_size:
                break
            cursor.execute('''
            INSERT INTO search_trends (keyword, platform, volume, date)
            VALUES (?, ?, ?, ?)
            ''', (keyword, 'Google', trend['value'], trend['time']))
            entries_stored += 1

    connection.commit()
    connection.close()
    print(f"Stored {entries_stored} entries for Google Trends keyword '{keyword}'!")

# Main execution
if __name__ == "__main__":
    setup_database()
    fetch_weather_data()
    fetch_google_trends('lemonade')
    fetch_google_trends('hot chocolate')
