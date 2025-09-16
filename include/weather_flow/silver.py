from __future__ import annotations
import io
import json
import unicodedata
from datetime import datetime, timezone
from typing import List, Dict, Any
import psycopg2, socket
from psycopg2.extras import execute_values
from airflow.hooks.base import BaseHook


import pandas as pd

from include.weather_flow.tasks import _get_minio_client


#! Bronze 1.0
BUCKET = "weather-data"

# Helper de texto/slug-Normalización de texto
def _slug(s:str)->str:
    if s is None:
        return None
    s = s.strip().lower()
    s = ''.join(c for c in unicodedata.normalize('NFD',s) if unicodedata.category(c) != 'Mn')
    s = s.replace(" ","-")
    return s

# Leemos los objetos JSON de Bronze en MinIO
def read_bronze_data(client, bucket: str, keys:List[str]) -> List[Dict[str, Any]]:
    records = []
    for key in keys:
        # Lee objetos a memoria (Podemos cambiar a streaming si es necesario)
        resp = client.get_object(bucket, key)
        try:
            data = resp.read()
        finally:
            resp.close()
            resp.release_conn()
    
        obj = json.loads(data.decode('utf-8'))
        records.append(obj)
    return records

# Normalizamos el DataFrame
def normalize_records(records: List[Dict[str, Any]], partition_date:str) -> pd.DataFrame:
    """
    partition_date: 'YYYY/MM/DD' (del DAG). La guardaremos como 'YYYY-MM-DD'
    """
    rows = []
    for r in records:
        city = r.get('city')
        coords = r.get('coords',{}) or {}
        main = r.get('main', {}) or {}
        weather_arr = r.get('weather',[]) or []
        wind = r.get('wind',{}) or {}
        rain = r.get('rain',{}) or {}
        clouds = r.get('clouds',{}) or {}
        exec_iso = r.get('execution_date')
        
        # Escogemos el Weather principal
        if weather_arr and isinstance(weather_arr, list):
            weather_main = weather_arr[0].get('main')
            weather_description = weather_arr[0].get('description')
        else:
            weather_main = None
            weather_description = None
        
        # Valores con default
        lat = coords.get('lat',None)
        lon = coords.get('lon',None)
        temp = main.get('temp',None)
        humidity = main.get('humidity',None)
        temp_min = main.get('temp_min',None)
        temp_max = main.get('temp_max',None)
        pressure = main.get('pressure',None)
        sea_level = main.get('sea_level',None)
        speed = wind.get('speed',None)
        rain_1h = 0.0
        
        if isinstance(rain, dict):
            #OWM usa "1h" 0 "3h" a veces; nos quedamos con 1h si existe
            if "1h" in rain and rain["1h"] is not None:
                rain_1h = float(rain["1h"])
        
        clouds_all = clouds.get('all')
        
        # requested_at_utc
        if exec_iso:
            # Parseamos el ISO con Z como +00:00
            dt = datetime.fromisoformat(exec_iso.replace("Z", "+00:00"))
            # Convertimos a UTC explícitamente
            requested_at_utc = dt.astimezone(timezone.utc)
        else:
            requested_at_utc = None
            
        rows.append({
            "city":city,
            "city_slug":_slug(city),
            "lat":lat,
            "lon":lon,
            "temp":temp,
            "humidity":humidity,
            "temp_min":temp_min,
            "temp_max":temp_max,
            "pressure":pressure,
            "sea_level":sea_level,
            "wind_speed":speed,
            "rain_1h":rain_1h,
            "clouds_all":clouds_all,
            "requested_at_utc":requested_at_utc,
            "partition_date":partition_date.replace("/","-"),
        })
        
    df = pd.DataFrame(rows)

    # Realizamos unas validaciones minimas de limpieza
    # 1. Tipos básicos/no negativos
    if not df.empty:
        # 'temp' puede ser negativa.
        df.loc[~df['humidity'].between(0,100,inclusive='both'),'humidity'] = None
        # Nulos razonables a 0:
        df['rain_1h'] = df['rain_1h'].fillna(0.0)
    
    cols = ['city','city_slug','lat','lon','temp','humidity','temp_min',
            'temp_max','pressure','sea_level','wind_speed','rain_1h','clouds_all',
            'requested_at_utc','partition_date']
    df = df.reindex(columns=cols)
    return df

# Escritura a Parquet/CSV en _tmp
def write_tmp_data(client, bucket:str, tmp_prefix:str, df: pd.DataFrame, fmt: str = "parquet"):
    """
    Escribe un único archivo consolidado por partición (se puede shardear si se prefiere)
    """
    out_name = "weather_silver.parquet" if fmt == "parquet" else "weather_silver.csv"
    key = f"{tmp_prefix.rstrip('/')}/{out_name}"
    
    if fmt == "parquet":
        bio = io.BytesIO()
        df.to_parquet(bio, index=False)
        bio.seek(0)
        client.put_object(bucket, key, data=bio, length=len(bio.getvalue()))
    else:
        data = df.to_csv(index=False).encode('utf-8')
        client.put_object(bucket, key, data=data, length=len(data))
    
    return {
        "tmp_key":key,
        "rows":len(df)
    }
    
def _normalize_to_silver(partition_date:str, files:list, tmp_prefix:str) -> dict:
    client = _get_minio_client()
    
    # Leemos bronze
    records = read_bronze_data(client, BUCKET, files)
    
    # Normalizamos
    df = normalize_records(records=records, partition_date=partition_date)
    
    # Validamos MinIO
    if df.empty:
        return {
            "rows":0,
            "tmp_key":None
        }
    
    # Escribimos en _tmp
    return write_tmp_data(client, BUCKET, tmp_prefix=f"silver/_tmp/weather/{partition_date}/",df=df,fmt="parquet")


# Cargue de información



def _load_silver_to_database(partition_date: str, parquet_key:str) -> dict:
    """
    Lee el parquet consolidado de MinIO y realiza un upsert a Supabase
    """
    client = _get_minio_client()
    response = client.get_object(BUCKET, parquet_key)
    data = response.read()
    response.close()
    response.release_conn()
    
    df = pd.read_parquet(io.BytesIO(data))
    
    # 1 Obtenemos la conexión a Supabase
    conn = BaseHook.get_connection("supabase_postgres")
    host = conn.host
    ipv4 = socket.gethostbyname(host)
    # Armar la cadena DSN manualmente psycopg2
    dsn = (
        f"dbname={conn.schema} "
        f"user={conn.login} "
        f"password={conn.password} "
        f"host={host} "          # necesario para TLS/SNI correcto
        f"hostaddr={ipv4} "      # fuerza IPv4 y evita AAAA
        f"port={conn.port or 5432} "
        f"sslmode=require"
    )
    
    # 2 Conectar y hacer upsert
    pg = psycopg2.connect(dsn)
    pg.autocommit = True
    cur = pg.cursor()
    
    # Antes de construir tuples:
    df = df.sort_values('requested_at_utc').drop_duplicates('city_slug', keep='last')

    
    # Preparamos el upsert
    tuples = list(df[["city","city_slug","lat","lon","temp","humidity","temp_min",
                      "temp_max","pressure","sea_level","wind_speed","rain_1h",
                      "clouds_all","requested_at_utc","partition_date"]].itertuples(index=False, name=None))
    insert_query = """
        INSERT INTO weather.weather_silver (
            city, city_slug, lat, lon, temp, humidity, temp_min, temp_max,
            pressure, sea_level, wind_speed, rain_1h, clouds_all,
            requested_at_utc, partition_date
        ) VALUES %s
        ON CONFLICT (city_slug, partition_date) DO NOTHING;
    """
    #temp = EXCLUDED.temp,
    #humidity = EXCLUDED.humidity,
    #temp_min = EXCLUDED.temp_min,
    #temp_max = EXCLUDED.temp_max,
    #pressure = EXCLUDED.pressure,
    #sea_level = EXCLUDED.sea_level,
    #wind_speed = EXCLUDED.wind_speed,
    #rain_1h = EXCLUDED.rain_1h,
    #clouds_all = EXCLUDED.clouds_all,
    #requested_at_utc = EXCLUDED.requested_at_utc;
    with pg, pg.cursor() as cur:
        execute_values(cur, insert_query, tuples)
    pg.commit()
    cur.close()
    pg.close()
    
    return {
        "rows_inserted":len(tuples)
    }
