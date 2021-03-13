import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain, cross_downstream

from datetime import datetime, timedelta

os.environ['USER'] = 'Massao Mitsunaga'

owner = os.environ.get('USER')

# simple code
#  dag = DAG(....)
#  task_1 = Operator(dag=dag)
#  task_2 = Operator(dag=dag)

# best practices
#  dag_id normalmente e o nome do arquivo
#  dates sempre em UTC
#  nunca deixe o start date variavel ex: datetime.now()
#  catchup = false ou True com max_active_runs ativo
#  1 task para limpar 1 task para carregar

# anotações
# scheduler_interval = None (gatilhado por algo externo(manual))
# timedelta = intervalo de tempo / cron = tempo absoluto (ex: 7 PM)
# Por padrão se o DAG fica parado por um tempo, assim que rodar
# novamente, ele vai rodar  todos os dags atrasados
# 1 operator = 1 task
# idenpotency (same input same output)

default_args = {
    'owner': owner,
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}


def _downloading_data(**kwargs):
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')


def _checking_data():
    print('check data')


with DAG(
        dag_id='simple_dag',
        default_args=default_args,
        schedule_interval=timedelta(minutes=2),
        start_date=datetime(2021, 3, 13),  # pode usar days_ago()
        catchup=False
) as dag:
    # task_1 = DummyOperator(
    #     task_id='task_1'
    # )

    # task_2 = DummyOperator(
    #     task_id='task_2'
    # )

    # task_3 = DummyOperator(
    #     task_id='task_3'
    # )
    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data
    )

    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
        poke_interval=5  # intervalo que vai checar o local
    )

    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 0'
    )

    # downloading_data.set_downstream(waiting_for_data)
    # waiting_for_data.set_downstream(processing_data)
    # processing_data.set_upstream(waiting_for_data)
    # waiting_for_data.set_upstream(downloading_data)

    # best practice (bit-shift operators)
    # downstream
    #downloading_data >> waiting_for_data >> processing_data
    # upstream
    #processing_data << waiting_for_data << downloading_data

    # Outras formas
    # Tarefas em paralelo
    #downloading_data >> [waiting_for_data, processing_data]

    # Seguencial usando chain()
    #chain(downloading_data, waiting_for_data, processing_data)

    # Dependencia cruzada entre as tarefas
    # cross_downstream([downloading_data, checking_data], [
    #                 waiting_for_data, processing_data])
