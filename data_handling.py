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

    # Create shared key tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS keywords (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword TEXT UNIQUE NOT NULL
    )
    ''')

    # Update weather_data with location_id
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        location_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        average_temp REAL NOT NULL,
        temperature_range TEXT NOT NULL,
        FOREIGN KEY (location_id) REFERENCES locations(id)
    )
    ''')

    # Update search_trends with keyword_id
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS search_trends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        keyword_id INTEGER NOT NULL,
        platform TEXT NOT NULL,
        volume INTEGER NOT NULL,
        date TEXT NOT NULL,
        FOREIGN KEY (keyword_id) REFERENCES keywords(id)
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
    base_url = "https://history.openweathermap.org/data/2.5/history/city"

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
            'start': 1514764800,
            'end': 1704067200,
            'appid': API_KEY,
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

                cursor.execute("INSERT OR IGNORE INTO locations (name) VALUES (?)", ("Michigan",))
                cursor.execute("SELECT id FROM locations WHERE name = ?", ("Michigan",))
                location_id = cursor.fetchone()[0]

                # Insert weather data with the location_id reference
                cursor.execute('''
                INSERT INTO weather_data (location_id, date, average_temp, temperature_range)
                VALUES (?, ?, ?, ?)
                ''', (location_id, current_date.isoformat(), avg_temp, temperature_range))
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


