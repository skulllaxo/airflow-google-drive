import pandas as pd

def convert_upper_to_lower(df:pd.DataFrame,columns:list) -> pd.DataFrame:
    for col in columns:
        df[col] = df[col].apply(str.lower)

    return df

df = pd.read_csv('11_Escolas_Coordenadas.csv',sep = ';')
sp_df = df[df['MUN'] == 'SAO PAULO']

del df
sp_df['CODVINC'] = sp_df['CODVINC'].fillna(0)
sp_df['CODVINC'] = sp_df['CODVINC'].astype('int64')

sp_df.NOMEDEP = sp_df.NOMEDEP.apply(str.replace,args = (' - ','_'))
sp_df.DE = sp_df.DE.apply(str.replace,args = (' ','_'))

sp_df = convert_upper_to_lower(sp_df,cols)
