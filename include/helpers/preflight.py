from airflow.decorators import task
from include.helpers.minio import get_minio_client

@task
def preflight(partition_day: str, force_rebuild: bool):
    client = get_minio_client()
    
    silver_prefix = f"silver/weather/{partition_day}"
    tmp_prefix = f"tmp/weather/{partition_day}"
    success_key = f"{silver_prefix}_SUCCESS"
    lock_key = f"{tmp_prefix}_LOCK"
    
    #Verificamos la existencia de SUCCESS
    if client.object_exists("weather-data", success_key) and not force_rebuild:
        return {
            "status":"SKIP",
            "silver_prefix":silver_prefix
        }
        
    # Verificamos la existencia de LOCK
    if client.object_existsI("wather-data",lock_key):
        raise AirflowSkipException("Another run is processing this partition")
    
    # Creamos el LOCK
    client.put_object("weather-data",lock_key,b"",lenght=0)
    client.remove_object("weather-data",tmp_prefix)
    
    # Devolvemos metadatos para tasks posteriores
    return{
        "status":"CONTINUE",
        "silver_prefix":silver_prefix,
        "tmp_prefix":tmp_prefix,
        "lock_key":lock_key
    }