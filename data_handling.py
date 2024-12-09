import urllib.request
import pandas as pd
import warnings

# Suppress warnings about the default style
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Define the API URL
key = "WJRFS5Y8NVKN5YSQ6SMAVZ3Z4"  # Replace with your actual API key
url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/history/2023-01-01/2024-01-01?unitGroup=us&include=days&key={key}&contentType=xlsx"
file_name = "weather_data_2023.xlsx"

# Step 1: Download the file
try:
    urllib.request.urlretrieve(url, file_name)
except Exception as e:
    print(f"Error downloading file: {e}")
    exit()

# Step 2: Load the file into pandas
try:
    data = pd.read_excel(file_name)  # Load the Excel file into a DataFrame
except Exception as e:
    print(f"Error loading Excel file: {e}")
    exit()

# Step 3: Extract the relevant columns
try:
    weather_data = data[['datetime', 'tempmax']].copy()  # Select only the date and high temperature columns

    # Rename columns for clarity
    weather_data.columns = ['date', 'high_temperature']

    # Print all cleaned data for the year
    print(weather_data.to_string(index=False))  # Print the entire DataFrame without the index
except KeyError as e:
    print(f"Column not found: {e}. Ensure the file contains 'datetime' and 'tempmax'.")
    exit()
except Exception as e:
    print(f"Error processing data: {e}")
    exit()

