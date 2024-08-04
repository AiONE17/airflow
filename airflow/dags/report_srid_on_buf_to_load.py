from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import psycopg2
import json

default_args = {
    'owner': 'Absalyamov',
    'start_date': datetime(2024, 8, 4)
}

dag = DAG(
    dag_id='report_srid_on_buf_to_load',
    default_args=default_args,
    schedule_interval='@daily',
    description='Даг для заполнения витрины на ПГ',
    catchup=False,
    max_active_runs=1
)

def main():

    client = Client('some-clickhouse-server',
                    user='absalyamov',
                    password='12345',
                    port=9000,
                    verify=False,
                    database= 'default',
                     settings={"numpy_columns": False,'usenumpy': True},
                     compression=False)

    main_table = "report.srid_on_buf_to_load"

    sql = f'''
        INSERT INTO {main_table}
        select toStartOfHour(dt) dt_hour,
               uniq(rid_hash) qty
        from stage.srid_tracker
        where action_id = 320
        group by dt_hour
    '''

    client.execute(sql)
    print(f'Запись в витрину данных {main_table} прошла успешно!')

    sql_pg = f'''
        select now() dt_last_load
            , dt_hour
            , qty
        from report.srid_on_buf_to_load final
    '''

    client_pg = psycopg2.connect(database="postgres", user="admin", password="admin",
                                   host="postgres_container", port="5432")


    df = client.query_dataframe(sql_pg)

    cursor = client_pg.cursor()

    df = df.to_json(orient="records", date_format="iso", date_unit="s")
    cursor.execute(f"CALL whsync.sridonbuftoload(_src := '{df}')")
    client_pg.commit()

    print('Импорт данных прошел успешно')

    cursor.close()
    client_pg.close()



task = PythonOperator(task_id='report_srid_on_buf_to_load', python_callable=main, dag=dag)
