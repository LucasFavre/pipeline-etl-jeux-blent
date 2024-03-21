import os

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Connection
from airflow import settings

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from pymongo import MongoClient


def create_conn(conn_id, conn_type, host, login, password, schema, port):
    """
    Check s'il y a une connection à la base de données déjà existante dans Airflow, et la crée sinon.

    """
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      login=login,
                      password=password,
                      schema=schema,
                      port=port)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        return None

    session.add(conn)
    session.commit()
    return conn


DAG_NAME = os.path.basename(__file__).replace(".py", "") 


default_args = {
    'owner': 'blent',
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'email_on_failure': True,
    'email': ['youremail@blent.ai'],
    'email_on_retry': False
}


def get_avis(**kwargs):

  client = MongoClient(kwargs['mongo_uri'], username = 'admin', password = kwargs['mongo_pw'])

  db = client.blent
  avis_jeux = db.avis_jeux

  date = datetime.strptime(kwargs['date'], '%Y-%m-%d')
  sixmonthsago = date - relativedelta(months = 6)

  match = {'$match': {'unixReviewTime': {'$gte': int(sixmonthsago.timestamp()), '$lte': int(date.timestamp())}}}

  sort = {'$sort': {'unixReviewTime': -1}}

  group = {
          '$group': {
              '_id': '$asin', 
              'average_rating': {
                  '$avg': '$overall'
              }, 
              'nb_ratings': {
                  '$sum': 1
              }, 
              'newest_rating': {
                  '$first': '$overall'
              }, 
              'oldest_rating': {
                  '$last': '$overall'
              }
          }
      }

  project = {
          '$project': {
              '_id': 1, 
              'average_rating': {
                  '$round': [
                      '$average_rating', 2
                  ]
              }, 
              'nb_ratings': 1, 
              'newest_rating': 1, 
              'oldest_rating': 1
          }
      }

  agg = [match, sort, group, project]

  def sql_string(value):
    if isinstance(value, str):
        return "'" + value + "'"
    return str(value)

  result_agg = avis_jeux.aggregate(agg)

  data = [list(r.values()) for r in result_agg]
  data = [[sql_string(v) for v in r] for r in data]
  data = ['(' + ', '.join(r) + ')' for r in data]
  data_string = ', '.join(data)

  print(data_string)

  ti = kwargs['ti']
  ti.xcom_push(key='sql_values', value = data_string)



@dag(DAG_NAME, default_args=default_args, schedule_interval="@daily", start_date=days_ago(1))
def dag_update_avis_jeux():


  # Si production -> Mettre sous variables airflow
  postgres_conn_id = 'my_postgres_connection'
  postgres_host = '18.202.251.80'
  postgres_pw = '89ibdYeycOjb'
  mongo_uri = "mongodb://18.203.138.218:27017?directConnection=true"
  mongo_pw = 'e5JGsSYUPD8lvxJk'

  create_conn(
      conn_id=postgres_conn_id,
      conn_type='postgres',
      host=postgres_host,
      login='postgres',
      password=postgres_pw,
      schema='jeux_blent',
      port=5432
  )


  table_name = 'top_jeux'

  insert_sql_template = f"""
  INSERT INTO {table_name}
  VALUES 
  """

  delete_sql = f'DELETE FROM {table_name};'

  @task()
  def show_date(**kwargs):
    print("La date d'exécution est : {}".format(kwargs["date"]))


  task_show_date = show_date(date="{{ ds }}")
  task_get_avis = PythonOperator(
    task_id='task_get_avis',
    python_callable=get_avis,
    provide_context=True,
    op_kwargs={'date' : "{{ ds }}", 'mongo_uri' : mongo_uri, 'mongo_pw' : mongo_pw}
    )
  task_delete_data = PostgresOperator(
    task_id='task_delete_data',
    postgres_conn_id=postgres_conn_id, 
    sql=delete_sql,
    )
  task_insert_data = PostgresOperator(
    task_id='task_insert_data',
    postgres_conn_id=postgres_conn_id,
    sql=insert_sql_template + "{{ ti.xcom_pull(task_ids='task_get_avis', key = 'sql_values') }}"
    )
    
   
  task_show_date.set_downstream(task_get_avis)  # date -> get_avis
  task_get_avis.set_downstream(task_delete_data) # get_avis -> delete_data
  task_delete_data.set_downstream(task_insert_data) # delete_date -> insert_data

dag_update_avis_jeux_instance = dag_update_avis_jeux()