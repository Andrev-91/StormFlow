from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from minio import Minio
from datetime import datetime
from unidecode import unidecode
import requests
from io import BytesIO
import json

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


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
                "execution_date":datetime.now().isoformat()+"Z"
                }
            )
        else:
            results.append({
                "city":city,
                "coords":{"lat":lat, "lon":lon},
                "status":f"http_{r.status_code}"
            })

    return results

def _store_weather_data(weather_data:list, execution_date:datetime):
    client = _get_minio_client()
    bucket_name = "weather-data"
    if not client.bucket_exists(bucket_name):
        print(f"Bucker {bucket_name} not found, creating it...")
        client.make_bucket(bucket_name)
        
    # Desglosamos las fechas para obtener rutas dinamicas
    year = execution_date.strftime('%Y')
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    
    
    for item in weather_data:
        city = item['city']
        ts = execution_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        data = json.dumps(item, ensure_ascii=False, default=str).encode("utf8")
        
        object_name = f"bronze/weather/{year}/{month}/{day}/{city}-{ts}.json"
        
        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=BytesIO(data),
            length=len(data)
        )
    return f's3://{bucket_name}/bronze/weather/{year}/{month}/{day}/'

#! Bronze 1.0
BUCKET = "weather-data"

def _list_bronze_data(partition_date:str)->dict:
    """
    Vamos a listar los datos en Bronze para una partición y devuelve métricas.
    """
    client = _get_minio_client()
    prefix = f"bronze/weather/{partition_date}"
    
    assert client.bucket_exists(BUCKET), f"Bucket {BUCKET} no existe"
    
    files = []
    total_bytes = 0
    
    for obj in client.list_objects(BUCKET, prefix=prefix, recursive=True):
        name = getattr(obj, "object_name", getattr(obj,"key",None))
        size = getattr(obj, "size", 0)
        if name and name.endswith(".json"):
            files.append(name)
            total_bytes += int(size or 0)
    
    count = len(files)
    if count == 0:
        raise AirflowFailException(
            f"No se encontraron JSONs en s3://{BUCKET}/{prefix}. Abortando particion {partition_date}"
        )
    files.sort()
    return {
        "partition_date": partition_date,
        "bronze_prefix": prefix,
        "files":files,
        "count":count,
        "bytes":total_bytes
    }