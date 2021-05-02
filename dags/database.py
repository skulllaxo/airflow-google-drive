
import psycopg2
import pandas as pd

class Database():
    def __init__(self):
        self.host = '127.0.0.1'
        self.dbname = 'escolas'
        self.user = 'escolas'
        self.password = 'escolas'
        self.port = 5433

        self.conn = psycopg2.connect(
            host = self.host,
            database = self.dbname,
            user = self.user,
            password = self.password,
            port = self.port,
            sslmode='disable')

        self.cursor = self.conn.cursor()

    def query_insert_generator(self,table_name:str,data_dict:dict) -> str:

        # - Example
        # data_dict = {
                    # 'id':1,
                    # 'municipio':'sao paulo',
                    # 'distrito':'sao paulo',
                    # 'nome_escola':'joao da silva'
                    # }

        cols = str([x for x in data_dict.keys()]).replace('[','(').replace(']',')')
        cols = cols.replace("'","")
        values = str([x for x in data_dict.values() if x]).replace('[','(').replace(']',')')
        
        if len(values) == 0 or len(cols) == 0:
            return ''
        else:
            query = f'INSERT INTO {table_name} {cols} VALUES {values}'
            print('!!!'*100)
            print(len(values))
            return query

    def convert_dataframe_to_list(self,dataframe:pd.DataFrame):
        js = list(df.T.to_dict().values())
        return js


    def run_query(self,query):
        if query != '':
        
            execute = self.cursor.execute(query)
            print('Query was executed')
            print('-' * 50)
            print(f'Query text: {query}')

    def commit(self):
        self.conn.commit()

    def close_connection(self):
        self.conn.close()

if __name__ == '__main__':
    df = pd.read_csv('load_files/Result.csv',sep = '\t')

    c = list(df.columns)
    c[0] = 'id'

    df.columns = c
    df = df.drop('id',axis = 1)

    db = Database()
    js = db.convert_dataframe_to_list(df)

    q = []
    for n,row in enumerate(js):

        query = db.query_insert_generator('enderecos',row)
        db.run_query(query)

    db.commit()
    db.close_connection()

    print(c)




