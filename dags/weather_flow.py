from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator


from datetime import datetime
import requests

from include.weather_flow.tasks import clean_city_list, _get_coordinates, _get_weather, _store_weather_data, _list_bronze_data
from include.weather_flow.silver import _normalize_to_silver, _load_silver_to_database

CITIES = ["Bogotá","Cali","Medellín","Cartagena","Barranquilla","Villavicencio","Armenia"]

cities = clean_city_list(CITIES)


# Configuramos el DAG
@dag(
    start_date=datetime(2025,1,1),
    schedule="@hourly",
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
    
    store_weather_data = PythonOperator(
        task_id="store_wather_data",
        python_callable=_store_weather_data,
        op_kwargs={'weather_data':get_weather.output}
    )
    
    def _ds_to_partition(ds:str)->str:
        return ds.replace("-","/")
    
    list_bronze = PythonOperator(
        task_id="list_bronze",
        python_callable = lambda ds, **_:_list_bronze_data(_ds_to_partition(ds)),
        provide_context=True
    )
    
    normalize_to_silver = PythonOperator(
        task_id = "normalize_to_silver",
        python_callable = lambda ti, **_:_normalize_to_silver(
            partition_date=ti.xcom_pull(task_ids="list_bronze")["partition_date"],
            files=ti.xcom_pull(task_ids="list_bronze")["files"],
            tmp_prefix=f"silver/_tmp/weather/{ti.xcom_pull(task_ids='list_bronze')['partition_date']}/",  
        ),
        provide_context = True,
    )
    load_silver_to_database = PythonOperator(
        task_id = "load_silver_to_database",
        python_callable = lambda ti, **_:_load_silver_to_database(
            partition_date=ti.xcom_pull(task_ids="list_bronze")["partition_date"],
            parquet_key=f"silver/_tmp/weather/{ti.xcom_pull(task_ids='list_bronze')['partition_date']}/weather_silver.parquet"
        ),
        provide_context = True,
    )
    
    check_api_status() >> get_coordinates >> get_weather >> store_weather_data >> list_bronze >> normalize_to_silver >> load_silver_to_database

weather_flow()      

