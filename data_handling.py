import requests
import pandas as pd

import time

def fetch_weather_data():
    weather_api_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/48104/2023-01-01/2024-01-01"
    params = {
        "elements": "datetime,tempmax",
        "include": "obs",
        "key": "C8P3R2GCZPUCQYRYDLDCPEZAZ",
        "contentType": "json"
    }
    
    retries = 5
    for i in range(retries):
        response = requests.get(weather_api_url, params=params)
        if response.status_code == 200:
            weather_data = response.json()
            temp_data = [{"date": day["datetime"], "tempmax": day["tempmax"]} for day in weather_data["days"]]
            weather_df = pd.DataFrame(temp_data)
            weather_df["date"] = pd.to_datetime(weather_df["date"])
            return weather_df
        elif response.status_code == 429:
            print(f"Rate limit exceeded. Retrying in {2**i} seconds...")
            time.sleep(2**i)  # Exponential backoff
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    raise Exception("Failed to fetch weather data after multiple retries.")

# Function to fetch Open-Meteo API data (Daily UV Index)
def fetch_uv_data():
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 44.31,
        "longitude": -85.6,
        "start_date": "2023-01-01",
        "end_date": "2024-01-01",
        "daily": "uv_index_max",
        "timezone": "America/New_York"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        uv_data = [{"date": pd.to_datetime(day["time"]), "uv_index_max": day["uv_index_max"]} for day in data["daily"]["time"]]
        uv_df = pd.DataFrame(uv_data)
        return uv_df
    else:
        raise Exception(f"Failed to fetch UV data: {response.status_code}")

# Function to fetch Google Trends data for a keyword
def fetch_trends(keyword):
    api_key = "4c668174a9439e8e26f6c233750fa3d50d8de4bd2d28fb757fb14b9eec28d111"
    url = "https://serpapi.com/search"
    params = {
        "engine": "google_trends",
        "q": keyword,
        "api_key": api_key
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        trend_data = [
            {"date": pd.to_datetime(trend["date"]), "volume": trend["values"][0]["extracted_value"]}
            for trend in data.get("interest_over_time", {}).get("timeline_data", [])
        ]
        return pd.DataFrame(trend_data)
    else:
        raise Exception(f"Failed to fetch trends for {keyword}: {response.status_code}")

def main():
    # Fetch all datasets
    weather_df = fetch_weather_data()
    uv_df = fetch_uv_data()
    hot_chocolate_df = fetch_trends("hot chocolate")
    lemonade_df = fetch_trends("lemonade")

    # Rename columns for clarity
    hot_chocolate_df.rename(columns={"volume": "hot_chocolate"}, inplace=True)
    lemonade_df.rename(columns={"volume": "lemonade"}, inplace=True)

    # Merge datasets
    combined_df = weather_df.merge(uv_df, on="date", how="inner")
    combined_df = combined_df.merge(hot_chocolate_df, on="date", how="left")
    combined_df = combined_df.merge(lemonade_df, on="date", how="left")
    combined_df.fillna({"hot_chocolate": 0, "lemonade": 0}, inplace=True)

    # Display combined data
    print(combined_df.head())

    # Optionally save the cleaned data to a CSV file for further analysis
    combined_df.to_csv("cleaned_combined_data.csv", index=False)
    print("Data saved to 'cleaned_combined_data.csv'")

# Ensure the main function runs only when this script is executed directly
if __name__ == "__main__":
    main()