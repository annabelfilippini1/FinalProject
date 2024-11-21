import requests
import time

API_KEY = 'f04f027d1faa2b1f808a1d516d743698'
BASE_URL = 'https://history.openweathermap.org/data/2.5/history/city?lat='  # Example endpoint

def fetch_weather_data(location):
    params = {'key': API_KEY, 'q': location}
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def collect_data(locations, data_points=100):
    weather_data = []
    for _ in range(data_points):
        for location in locations:
            data = fetch_weather_data(location)
            weather_data.append(data)
            time.sleep(1)  # To handle rate limits
    return weather_data
