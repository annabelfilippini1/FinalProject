import urllib.request
import json
import pandas as pd

# Define the API URL
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/michigan/2023-01-01/2023-12-31?elements=datetime%2Ctempmax%2Ctempmin&include=days&key=L5S34V39G8SNE7QB9SWETFPRD&contentType=json"

try:
    # Make the API request
    response = urllib.request.urlopen(url)
    
    # Parse the JSON response
    jsonData = json.load(response)
    
    # Extract daily data
    daily_data = jsonData.get("days", [])
    
    # Create a DataFrame with the desired fields
    weather_data = pd.DataFrame([
        {"date": day["datetime"], "high_temperature": day["tempmax"]}
        for day in daily_data
    ])
    
    # Print every day and its high temperature
    print(weather_data.to_string(index=False))  # Print all rows without an index column

    # Optionally save to a CSV file
    weather_data.to_csv("michigan_weather_2023.csv", index=False)
    print("Data saved to 'michigan_weather_2023.csv'")
    
except urllib.error.HTTPError as e:
    error_info = e.read().decode()
    print("HTTP Error:", e.code, error_info)
except urllib.error.URLError as e:
    print("URL Error:", e.reason)
except Exception as e:
    print("An error occurred:", e)
