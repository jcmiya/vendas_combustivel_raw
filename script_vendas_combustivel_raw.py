import pandas as pd
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine

#lê arquivo
dic_df = {}
for i in range(3):
  dic_df[i] = pd.read_excel('vendas-combustiveis-m3-1.xls',sheet_name=i)

#cria df diesel
df_diesel = dic_df[1]

df_diesel = df_diesel.drop(columns=['TOTAL'])

df_diesel = pd.melt(df_diesel,id_vars=df_diesel.columns[:-12],value_vars=df_diesel.columns[-12:], var_name='mes', value_name='volume')

#formata mês
dic_mes = {'Jan':1,'Fev':2,'Mar':3,'Abr':4,'Mai':5,'Jun':6,'Jul':7,'Ago':8,'Set':9,'Out':10,'Nov':11,'Dez':12}

df_diesel['mes'] = df_diesel['mes'].apply(lambda x: dic_mes[x])

df_diesel['year_month'] = pd.to_datetime(df_diesel['ANO'].astype(str) + '-' + df_diesel['mes'].astype(str))

df_diesel = df_diesel.drop(columns = ['ANO','mes'])

df_diesel = df_diesel.rename(columns={'COMBUSTÍVEL':'product','ESTADO':'uf','UNIDADE':'unit'}).drop(columns = ['REGIÃO'])

import datetime
df_diesel['created_at'] = datetime.datetime.now()

df_diesel['category'] = 'Diesel'

print(df_diesel)

#para consultar o df inteiro
"""
with pd.option_context('display.max_rows', None, 'display.max_columns', None):  
  print(df_diesel)
"""
print(df_diesel.dtypes)

#cria df gasolina
df_derivado = dic_df[2]

df_derivado = df_derivado.drop(columns=['TOTAL'])

df_derivado = pd.melt(df_derivado,id_vars=df_derivado.columns[:-12],value_vars=df_derivado.columns[-12:], var_name='mes', value_name='volume')

#formata mês
dic_mes = {'Jan':1,'Fev':2,'Mar':3,'Abr':4,'Mai':5,'Jun':6,'Jul':7,'Ago':8,'Set':9,'Out':10,'Nov':11,'Dez':12}

df_derivado['mes'] = df_derivado['mes'].apply(lambda x: dic_mes[x])

df_derivado['year_month'] = pd.to_datetime(df_derivado['ANO'].astype(str) + '-' + df_derivado['mes'].astype(str))

df_derivado = df_derivado.drop(columns = ['ANO','mes'])

df_derivado = df_derivado.rename(columns={'COMBUSTÍVEL':'product','ESTADO':'uf','UNIDADE':'unit'}).drop(columns = ['REGIÃO'])

import datetime

df_derivado['created_at'] = datetime.datetime.now()

df_derivado['category'] = 'Derivado'

print(df_derivado)

#grava banco de dados
#parametros conexão db
host_name = 'localhost'
db_name = 'vendas'
u_name = 'root'
u_pass = 'ta@bEELm'
port_num= '3306'

#my_eng = create_engine('mysql+mysqlconnector://[user]:[pass]@[host]:[port]/[schema]',echo=False)
#my_eng = create_engine("mysql+mysqlconnector:// + u_name + ':' + u_pass + '@' + host_name + ':' + port_num")
my_eng = create_engine('mysql+mysqlconnector://' + u_name + ':' + u_pass + '@' + host_name + ':' + port_num)

#grava df_diesel na tabela
df_diesel.to_sql(name='vendas_combustiveis', con=my_eng, if_exists='append', index=False)
print("Dados inseridos")

#grava df_derivado na tabela
df_derivado.to_sql(name='vendas_combustiveis', con=my_eng, if_exists='append', index=False)
print("Dados inseridos")