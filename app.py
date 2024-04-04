import json
import random
import time
import numpy as np
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
from flask import Flask, jsonify, request

# Azure Blob Storage connection string and container name
connection_string = 'DefaultEndpointsProtocol=https;AccountName=pocdatastorage07;AccountKey=LlLiJ3sXrrtyIMYlR+bSBV3KqWhWnboDvPzphi5K64HeyeLoTZ+tqJzjXYjncdU6vdf1ymneBR4V+ASturzEZA==;EndpointSuffix=core.windows.net'  
container_name = 'weather-data'

app = Flask(__name__)

# Define latitude and longitude ranges for each city
city_coordinates_range = {
    'New York': {'latitude_range': (40.5, 41.0), 'longitude_range': (-74.3, -73.7)},
    'London': {'latitude_range': (51.3, 51.7), 'longitude_range': (-0.5, 0.1)},
    'Tokyo': {'latitude_range': (35.5, 35.8), 'longitude_range': (139.5, 140.0)},
    'Sydney': {'latitude_range': (-34.0, -33.5), 'longitude_range': (151.0, 151.5)},
    'Paris': {'latitude_range': (48.7, 49.1), 'longitude_range': (2.1, 2.6)}
}

# Function to generate synthetic weather data for a given city
def generate_weather_data(city):
    # Simulate weather conditions with variability
    weather_conditions = ['Clear', 'Clouds', 'Rain', 'Snow', 'Thunderstorm']
    random_range = round(random.randint(200, 400), 3)
    data = []
    for _ in range(random_range):  # Generate random records per city
        temperature = round(random.uniform(-10, 40), 2)  # Temperature in Celsius
        humidity = round(random.uniform(20, 90), 2)  # Humidity in percentage
        pressure = round(random.uniform(980, 1050), 2)  # Atmospheric pressure in hPa
        weather = random.choice(weather_conditions)

        # Introduce outliers and NaN values
        if random.random() < 0.05:  # 5% chance of outliers
            temperature = round(random.uniform(100, 200), 2)
        if random.random() < 0.05:  # 5% chance of NaN values
            temperature = np.nan

        if random.random() < 0.05:  # 5% chance of outliers
            humidity = round(random.uniform(100, 110), 2)
        if random.random() < 0.05:  # 5% chance of NaN values
            humidity = np.nan

        if random.random() < 0.05:  # 5% chance of outliers
            pressure = round(random.uniform(50, 100), 2)
        if random.random() < 0.05:  # 5% chance of NaN values
            pressure = np.nan
        # Generate random latitude and longitude within specified range for the city
        latitude = round(random.uniform(city_coordinates_range[city]['latitude_range'][0], 
                                         city_coordinates_range[city]['latitude_range'][1]), 6)
        longitude = round(random.uniform(city_coordinates_range[city]['longitude_range'][0], 
                                          city_coordinates_range[city]['longitude_range'][1]), 6)

        # Create timestamp for current time with slight variation
        timestamp = (datetime.now() + timedelta(seconds=random.randint(-300, 300))).strftime('%Y-%m-%d %H:%M:%S')

        data.append({
            'city': city,
            'timestamp': timestamp,
            'temperature': temperature,
            'humidity': humidity,
            'pressure': pressure,
            'weather': weather,
            'latitude': latitude,
            'longitude': longitude
        })

    return data

# Function to store weather data in Azure Blob Storage
def store_weather_data(data):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(f'weather_data_{datetime.now()}.json')
    blob_client.upload_blob(json.dumps(data))
    # pass

# Function to continuously generate and store weather data
def generate_and_store_weather_data(cities, interval):
    all_data = []
    for city in cities:
        data = generate_weather_data(city)
        all_data.extend(data)
        print(f"Weather data for {city} generated.")
        time.sleep(interval)
    store_weather_data(all_data)
    print("All weather data generated and stored.")
    return all_data

@app.route('/weather_data', methods=['POST'])
def weather_data():
    if request.method == 'POST':
        # get json data from request
        request_data = request.json
        cities = request_data.get('cities')
        interval = request_data.get('interval')

        if not cities or not interval:
            return jsonify({'Error': 'Cities and interval are required.'}), 400
        
        # Trigger weather data generation
        data = generate_and_store_weather_data(cities, interval)

        return jsonify({'Data': data, 'Message': 'Weather data generation completed.'}), 200


if __name__ == '__main__':
    # Generate weather data for multiple cities and store in Azure Blob Storage
    app.run()

