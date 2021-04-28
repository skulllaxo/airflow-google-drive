from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
import pandas as pd



def create_dataframe(path:str,**kwargs):
    df = pd.read_csv(path,sep = ';',encoding = "ISO-8859-1")
    data = df.to_dict()

    ti = kwargs['ti']
    ti.xcom_push(key = 'raw_dataframe',value = data)

def filter_data_by_char(key_data:str,col:str,char:str,**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'raw_dataframe')

    df = pd.DataFrame(data)
    df = df[df[col] == char]

    data = df.to_dict()

    ti.xcom_push(key = key_data,value = data)

    
def convert_columns(key_data:str,**kwargs):
    def convert_upper_to_lower(df:pd.DataFrame,columns:list) -> pd.DataFrame:
        for col in columns:
            print(10*"@")
            print(col)
            df[col] = df[col].apply(str).apply(str.lower) 

        return df

    ti = kwargs['ti']
    data = ti.xcom_pull(key = key_data)

    df = pd.DataFrame(data)

    df['CODVINC'] = df['CODVINC'].fillna(0)
    df['CODVINC'] = df['CODVINC'].astype('int64')

    df.NOMEDEP = df.NOMEDEP.apply(str.replace,args = (' - ','_'))
    df.DE = df.DE.apply(str.replace,args = (' ','_'))

    cols = ['NOMEDEP','DE','MUN','DISTR','BAIESC','ZONA','NOMESC','SITUACAO','TIPOESC','ENDESC','COMPLEND','BAIESC','ZONA']
    df = convert_upper_to_lower(df,cols)

    data = df.to_dict()

    ti.xcom_push(key = 'dataframe_formated',value = data)

default_args = {
    'owner': 'Andre',
    'start_date': datetime(2021, 4,23),
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG(dag_id = 'School',default_args = default_args,schedule_interval = timedelta(minutes=10),catchup=False) as dag:
    task_dataframe = PythonOperator(
    task_id = 'create_dataframe',
    python_callable = create_dataframe,
    op_kwargs = {'path':'/usr/local/airflow/load_files/School.csv'},
    dag = dag)

    task_filter = PythonOperator(
    task_id = 'filter_dataframe',
    python_callable = filter_data_by_char,
    op_kwargs = {'key_data':'filted_dataframe','char':'SAO PAULO','col':'MUN'},
    dag = dag)

    task_convert = PythonOperator(
    task_id = 'convert_dataframe',
    python_callable = convert_columns,
    op_kwargs = {'key_data':'filted_dataframe'},
    dag = dag)

task_dataframe >> task_filter >> task_convert