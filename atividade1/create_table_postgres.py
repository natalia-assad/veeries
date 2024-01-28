#código apenas para a criação das tabelas no postgres
# Supondo que você já tenha um DataFrame chamado df 
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')
conn_string = os.getenv('conn_string')

#considerando um dataframe "df" já criado

def create_table_postgres(tabela):
    # Conectar ao PostgreSQL
    conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port)
    # Criar uma tabela a partir do DataFrame
    engine = create_engine(conn_string)
    df.to_sql(tabela, engine, if_exists='replace', index=False)  # Substitua 'nome_da_tabela' pelo nome desejado para a tabela
    # Fechar a conexão
    conn.close()
