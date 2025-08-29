from airflow.hooks.base import BaseHook
from datetime import datetime
from unidecode import unidecode
import requests
import json


def clean_city_list(cities) -> list:
    cleaned_cities = []
    for city in cities:
        no_accent = unidecode(city)
        lower = no_accent.lower()
        cleaned_cities.append(lower)
    
    print(cleaned_cities)   
    return cleaned_cities
        
#! Referencia del llamado de API que se quiere lograr
"""http://api.openweathermap.org/geo/1.0/direct?q={city name}&limit={limit}&appid={API key}"""
    
def _get_coordinates(cities):
    api = BaseHook.get_connection("weather_api")
    results = []
    for city in cities:
        url=f"{api.host}{api.extra_dejson['endpoints']['geocoding']}"
        default_params = {"units":"metric","lang":"es"}
        merged_params = { **default_params, "q": city, "limit":1, "appid":api.password }
        r = requests.get(url, params=merged_params, headers=api.extra_dejson['headers'])
        if r.status_code == 200:
            items = r.json()
            if items:
                first = items[0]
                lat = first['lat']
                lon = first['lon']
                results.append({
                    "city":city,
                    "lat":lat,
                    "lon":lon,
                    "status":"ok"
                })
        else:
            results.append({
                "city":city,
                "status":f"http_{r.status_code}"
            })
    return results

"""https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}"""

def _get_weather(coords_list):
    api = BaseHook.get_connection("weather_api")
    url=f"{api.host}{api.extra_dejson['endpoints']['weather']}"
    results = []
    for item in coords_list:
        city = item['city']
        lat = item['lat']
        lon = item['lon']
        default_params = {"units":"metric","lang":"es"}
        merged_params = { **default_params, "lat":lat,"lon":lon,"appid":api.password }
        r = requests.get(url=url, params=merged_params, headers=api.extra_dejson['headers'])
        if r.status_code == 200:
            data = r.json()
            w = (data.get("weather")or [{}][0])
            m = data.get("main",{})
            wd = data.get("wind",{})
            rn = data.get("rain",{})
            rn1h = rn.get("1h",0)
            cl = data.get("clouds",{})
            results.append({
                "city":city,
                "coords":{
                    "lat":lat,
                    "lon":lon
                    },
                "name":data["name"],
                "main":m,
                "weather":w,
                "wind":wd,
                "rain":rn,
                "rain_1h":rn1h,
                "clouds":cl,
                "requested_at_utc":datetime.now().astimezone()
                }
            )
        else:
            results.append({
                "city":city,
                "coords":{"lat":lat, "lon":lon},
                "status":f"http_{r.status_code}"
            })

    return results