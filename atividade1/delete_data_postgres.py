import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')


def delete_postgres(year:int,tabela):

    # Conectar ao banco de dados
    conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port,
    )

    cur = conn.cursor()
    query = sql.SQL("""
        delete from public.{tabela}
                    where ano = {year}
        """.format(tabela=tabela, year=year))
        # Executar a query com os valores correspondentes
    cur.execute(query)
    # Commit (salvar) as mudanças
    conn.commit()

    # Fechar a conexão
    cur.close()
    conn.close()

    return print('Deletado dados de {year} da tabela {tabela}'.format(year=year,tabela=tabela))


#execução
delete_postgres(2018,'area_colhida')