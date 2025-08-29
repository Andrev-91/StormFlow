from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

from include.weather_flow.tasks import clean_city_list, _get_coordinates, _get_weather

CITIES = ["Bogotá","Cali","Medellín","Cartagena"]

cities = clean_city_list(CITIES)


# Configuramos el DAG
@dag(
    start_date=datetime(2025,1,1),
    schedule="@daily",
    catchup=False,
    tags=["weather_flow"]
)

# Creamos la funcion principal del DAG
def weather_flow():
    # Creamos la funcíón para verificar que el API este disponible
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def check_api_status():
        api=BaseHook.get_connection("weather_api")
        url=f"{api.host}{api.extra_dejson['endpoints']['weather']}"
        print(url)
        params = {"lat":4.61,"lon":-74.08, "appid":api.password}
        response = requests.get(url, params=params, headers=api.extra_dejson['headers'])
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_coordinates = PythonOperator(
        task_id="get_coordinates",
        python_callable=_get_coordinates,
        op_kwargs={'cities':cities}
    ) 
    
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=_get_weather,
        op_kwargs={"coords_list":get_coordinates.output}
    )
    
    check_api_status() >> get_coordinates >> get_weather

weather_flow()      

