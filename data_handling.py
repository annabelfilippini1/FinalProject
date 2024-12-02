import sqlite3
import requests
import datetime

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
    API_KEY = 'f04f027d1faa2b1f808a1d516d743698'
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
        timestamp = int(current_date.strftime('%s'))  # Convert date to UNIX timestamp
        params = {
            'lat': lat,
            'lon': lon,
            'dt': timestamp,
            'appid': API_KEY
        }

        response = requests.get(base_url, params=params)
        data = response.json()

        if 'current' in data:
            current = data['current']
            avg_temp = current['temp']
            temp_min = current.get('temp_min', avg_temp)
            temp_max = current.get('temp_max', avg_temp)
            temperature_range = f"{temp_min} - {temp_max}"

            cursor.execute('''
            INSERT INTO weather_data (date, average_temp, temperature_range)
            VALUES (?, ?, ?)
            ''', (current_date.isoformat(), avg_temp, temperature_range))

        current_date += datetime.timedelta(days=1)

    connection.commit()
    connection.close()
    print(f"Stored {batch_size} weather data entries successfully!")

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

# Fetch Twitter Data
def fetch_twitter_data(keyword):
    BEARER_TOKEN = 'your_twitter_bearer_token'  # Replace with actual Bearer Token
    base_url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}'
    }
    params = {
        'query': keyword,
        'tweet.fields': 'created_at',
        'max_results': 100
    }

    response = requests.get(base_url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error: Unable to fetch Twitter data for {keyword}. HTTP Status Code: {response.status_code}")
        return

    data = response.json()

    connection = sqlite3.connect('data/sip_and_search.sqlite')
    cursor = connection.cursor()

    if 'data' in data:
        for tweet in data['data']:
            cursor.execute('''
            INSERT INTO search_trends (keyword, platform, volume, date)
            VALUES (?, ?, ?, ?)
            ''', (keyword, 'Twitter', 1, tweet['created_at']))

    connection.commit()
    connection.close()
    print(f"Twitter data for '{keyword}' fetched and stored successfully!")

# Main execution
if __name__ == "__main__":
    setup_database()
    fetch_weather_data()
    fetch_google_trends('lemonade')
    fetch_google_trends('hot chocolate')
    fetch_twitter_data('lemonade')
    fetch_twitter_data('hot chocolate')