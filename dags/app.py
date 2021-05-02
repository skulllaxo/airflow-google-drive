from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta,datetime
from database import Database

from drive import Drive
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

    
def convert_columns_type(**kwargs):


    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'filted_dataframe')

    df = pd.DataFrame(data)

    df['CODVINC'] = df['CODVINC'].fillna(0)
    df['CODVINC'] = df['CODVINC'].astype('int64')

    df.NOMEDEP = df.NOMEDEP.apply(str.replace,args = (' - ','_'))
    df.DE = df.DE.apply(str.replace,args = (' ','_'))

    data = df.to_dict()

    ti.xcom_push(key = 'dataframe_typed',value = data)

def rename_columns(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(key = 'data_frame_lowercase')

        df = pd.DataFrame(data)
        df.columns = ["nome_rede_ensino",
                  "diretoria_ensino",
                  "municipio",
                  "distrito",
                  "cod_escola",
                  "nome_escola",
                  "situacao_escola",
                  "tipo_escola",
                  "endereco_escola",
                  "numero_endereco",
                  "complemento",
                  "cep",
                  "bairro",
                  "zona_urbana",
                  "longitude",
                  "latitude",
                  "cod_vinculadora"]

        data = df.to_dict()

        ti.xcom_push(key = 'dataframe_named_header',value = data )



def convert_uppercase_to_lowercase(**kwargs):
    cols = ['NOMEDEP','DE','MUN','DISTR','BAIESC','ZONA','NOMESC','SITUACAO','TIPOESC','ENDESC','COMPLEND','BAIESC','ZONA']
    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'dataframe_typed')
    df = pd.DataFrame(data)

    for col in cols:
        df[col] = df[col].apply(str).apply(str.lower) 

    data = df.to_dict()
    ti.xcom_push(key = 'data_frame_lowercase',value = data)


def select_columns(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'dataframe_named_header')
    df = pd.DataFrame(data)

    df = df[['municipio','nome_escola','endereco_escola','numero_endereco','cep','bairro','longitude','latitude']]
    ti.xcom_push(key = 'dataframe_columns_selected',value = df.to_dict())

def null_values(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'dataframe_columns_selected')

    df = pd.DataFrame(data)

    df = df[df['latitude'].notna()]
    df = df[df['longitude'].notna()]

    df['municipio'] = df['municipio'].fillna('NULL')
    df['endereco_escola'] = df['endereco_escola'].fillna('NULL')
    df['numero_endereco'] = df['numero_endereco'].fillna('NULL')
    df['cep'] = df['cep'].fillna(0)
    df['bairro'] = df['bairro'].fillna('NULL')


    ti.xcom_push(key = 'dataframe_null_values', value = df.to_dict())


    

def write_dataframe_to_csv(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'dataframe_null_values')

    df = pd.DataFrame(data)

    df.to_csv('load_files/Result.csv',sep = '\t')


def upload_file(**kwargs):
    drive = Drive()
    drive.upload_file(folder_id ='1wCQlfB-q1Aj6AceKz5RLBExMo9bJFutc',file_name = 'load_files/Result.csv',title = 'ETL_school.csv')

def insert_into_database(**kwargs):

    ti = kwargs['ti']
    data = ti.xcom_pull(key = 'dataframe_null_values')
    df = pd.DataFrame(data)

    c = list(df.columns)
    c[0] = 'id'

    df.columns = c

    #remove column id
    df = df.drop('id',axis = 1)

    #creates database connection
    db = Database()

    #convert dataframe to list
    data_list = db.convert_dataframe_to_list(df)

    
    for n,row in enumerate(data_list):

        #creates sql insert query
        query = db.query_insert_generator('enderecos',row)

        #execute sql query
        db.run_query(query)
        
    db.commit()
    db.close_connection()

default_args = {
    'owner': 'Andre',
    'start_date': datetime(2021, 5,1),
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

    task_convert_type = PythonOperator(
    task_id = 'convert_columns_type',
    python_callable = convert_columns_type,
    dag = dag)

    task_to_lowercase = PythonOperator(
    task_id = 'convert_to_lowercase',
    python_callable = convert_uppercase_to_lowercase,
    dag = dag
    )

    task_columns_names = PythonOperator(
    task_id = 'rename_columns',
    python_callable = rename_columns,
    dag = dag
    )

    task_select_columns = PythonOperator(
    task_id = 'select_columns',
    python_callable = select_columns,
    dag = dag
    )

    task_null_values = PythonOperator(
    task_id = 'null_values',
    python_callable = null_values,
    dag = dag
    )


    task_write_csv = PythonOperator(
    task_id = 'write_csv',
    python_callable = write_dataframe_to_csv,
    dag = dag
    )

    task_insert_into_database = PythonOperator(
    task_id = 'insert_into_database',
    python_callable = insert_into_database,
    dag = dag
    )

    task_upload_file = PythonOperator(
    task_id = 'upload_file',
    python_callable = upload_file,
    dag = dag)
    
    
    
    
    

task_dataframe >> task_filter >> task_convert_type >> task_to_lowercase >> task_columns_names >> task_select_columns >> task_null_values >> [task_write_csv,task_insert_into_database,task_upload_file]