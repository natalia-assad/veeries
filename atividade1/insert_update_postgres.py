#bibliotecas
import pandas as pd
import requests
from unidecode import unidecode
import numpy as np
import psycopg2
from psycopg2 import sql
from datetime import datetime
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

###################SUB-FUNÇÕES#################################

#realiza a conexão com a api, e transforma o json em dataframe
def consulta_api(url):
    # Fazendo a requisição GET
    response = requests.get(url)
    if response.status_code == 200:
        print("requisição bem sucedida")
        data = response.json()
        #transforma em dataframe
        df = pd.DataFrame(data)
    else:
        print(f"A requisição falhou com o código de status {response.status_code}")
    return df

#faz as transformações necessárias para os dados e retorna o dataframe pronto para ser imputado
def organize_df(df):
    #renomeia as colunas do dataframe como a primeira linha
    new_columns = df.iloc[0]  
    df = df[1:] 
    df.columns = new_columns 
    if df.shape[0]>0:
        #realiza ajustes nos nomes da coluna, para normalizá-los
        df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.lower().str.replace(' ', '_')
        df = df.rename(columns=lambda x: x.replace('(codigo)', 'id'))
        #separa o municipio do estado
        df[['municipio', 'estado']] = df['municipio'].str.split(' - ', expand=True)
        #padroniza os dados para minusculo e sem acento
        df[df.columns.difference(['estado'])] = df[df.columns.difference(['estado'])].applymap(lambda x: unidecode(str(x)).lower())
        #Converter para inteiros, transformando valores não numéricos em NaN
        colunas_para_int = ['nivel_territorial_id', 'unidade_de_medida_id','valor','municipio_id','variavel_id', 'ano_id', 'ano','produto_das_lavouras_temporarias_e_permanentes_id']
        for coluna in colunas_para_int:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce')
    else:
        print("Não há dados disponíveis para o ano selecionado")
        pass
    return df


#com base no dataframe gerado realiza as operações no postgres
def insert_or_update_postgres(df,tabela):
    # Conectar ao banco de dados
    conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port,
    )
    # Substituir valores NaN por None. Apenas como None o postgres reconhece como valor ausente
    df.replace({np.nan: None}, inplace=True)
    cur = conn.cursor()
    for index, row in df.iterrows():
        query = sql.SQL("""
        INSERT INTO public.{tabela} 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (municipio_id,ano_id) DO UPDATE
        SET
            nivel_territorial_id = EXCLUDED.nivel_territorial_id,
            nivel_territorial = EXCLUDED.nivel_territorial,
            unidade_de_medida_id = EXCLUDED.unidade_de_medida_id,
            unidade_de_medida = EXCLUDED.unidade_de_medida,
            valor = EXCLUDED.valor,
            municipio_id = EXCLUDED.municipio_id,
            municipio = EXCLUDED.municipio,
            variavel_id = EXCLUDED.variavel_id,
            variavel = EXCLUDED.variavel,
            ano_id = EXCLUDED.ano_id,
            ano = EXCLUDED.ano,
            produto_das_lavouras_temporarias_e_permanentes_id = EXCLUDED.produto_das_lavouras_temporarias_e_permanentes_id,
            produto_das_lavouras_temporarias_e_permanentes = EXCLUDED.produto_das_lavouras_temporarias_e_permanentes,
            estado = EXCLUDED.estado;
        """.format(tabela=tabela))
        # Executar a query com os valores correspondentes
        cur.execute(query, (
            row['nivel_territorial_id'],
            row['nivel_territorial'],
            row['unidade_de_medida_id'],
            row['unidade_de_medida'],
            row['valor'],
            row['municipio_id'],
            row['municipio'],
            row['variavel_id'],
            row['variavel'],
            row['ano_id'],
            row['ano'],
            row['produto_das_lavouras_temporarias_e_permanentes_id'],
            row['produto_das_lavouras_temporarias_e_permanentes'],
            row['estado']
        ))
    # Commit (salvar) as mudanças
    conn.commit()
    # Fechar a conexão
    cur.close()
    conn.close()
    return print('Inserção ou Atualização concluídas')

#########################FUNÇÃO PRINCIPAL ############################################

#função completa, inicia com a conexão a api e finaliza com a iteiração com o banco de dados
def insert_or_update(tabela:str, year: int, **kwargs):
    if tabela == 'area_colhida':
        url = 'https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/216/p/{ano}/c782/40124?formato=json'.format(ano=year)
    elif tabela == 'quantidade_produzida':
        url = 'https://apisidra.ibge.gov.br/values/t/5457/n6/all/v/214/p/{ano}/c782/40124?formato=json'.format(ano=year)
    else:
        print('url não disponível para esta tabela. As opções de tabela são: area_colhida, quantidade_produzida')
        pass
    print("Iniciando conexão a API {tabela} - {ano}".format(tabela=tabela,ano=year))
    df_tabela = consulta_api(url)
    df = organize_df(df_tabela)
    if not df.empty:  # Check if the DataFrame is not empty
        send = insert_or_update_postgres(df, tabela)
    else:
        print("DataFrame vazio. Não será realizada a inserção/atualização no PostgreSQL.")
    return print("Processo finalizado para tabela {tabela} - {ano}".format(tabela=tabela,ano=year))

######EXECUÇÃO#########################

#cria uma lista com os anos desde 2018 até o ano corrente
#Para cada ano da lista, a função insert_or_update é realizada
ano_corrente = datetime.now().year
anos = list(range(2018, ano_corrente + 1))
for i in anos:
    dale = insert_or_update('area_colhida',year=i)
    dale = insert_or_update('quantidade_produzida',year=i)