
import psycopg2

host = '127.0.0.1'
dbname = 'school'
user = 'airflow'
password = 'airflow'

conn = psycopg2.connect(
    host=host,
    database=dbname,
    user=user,
    password=password)
conn.execute('')