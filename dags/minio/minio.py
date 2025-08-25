from airflow.hooks.base import BaseHook

def get_minio_conn(conn_id='minio'):
    conn = BaseHook.get_connection(conn_id)
    # endpoint_url은 extra에 저장됨
    endpoint = conn.extra_dejson.get('endpoint_url')
    access_key = conn.login
    secret_key = conn.password
    return endpoint, access_key, secret_key