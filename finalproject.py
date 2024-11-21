import requests

API_KEY = 'f04f027d1faa2b1f808a1d516d743698'
BASE_URL = 'https://history.openweathermap.org/data/2.5/history/city?lat='

def fetch_weather_data(lat, lon, start, end):
    # Add the required parameters (latitude, longitude, start date, end date, and API key)
    params = {
        'lat': lat,
        'lon': lon,
        'type': 'hour',  # You can change this to 'day' if you're looking for daily data
        'start': start,
        'end': end,
        'appid': API_KEY
    }
    
    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        return response.json()  # Return the JSON data
    else:
        print(f"Error: {response.status_code}")
        response.raise_for_status()

def print_weather_data(lat, lon, start, end):
    weather_data = fetch_weather_data(lat, lon, start, end)
    
    # Loop through the historical weather data and print the temperature
    for entry in weather_data['list']:
        temp_f = entry['main']['temp'] * 9/5 - 459.67  # Convert from Kelvin to Fahrenheit
        timestamp = entry['dt']  # Timestamp of the data point
        print(f"At timestamp {timestamp}, the temperature was {temp_f:.2f}°F.")

# Example usage (you'll need to provide the latitude and longitude, and the start and end timestamps)
lat = 42.2808  # Latitude for Ann Arbor
lon = -83.7430  # Longitude for Ann Arbor
start_timestamp = 1633046400  # Example start timestamp (Unix timestamp)
end_timestamp = 1633132800  # Example end timestamp (Unix timestamp)

print_weather_data(lat, lon, start_timestamp, end_timestamp)
