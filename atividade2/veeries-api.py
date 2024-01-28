import psycopg2
from flask import Flask, jsonify
from dotenv import load_dotenv
import os

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()
dbname = os.getenv('dbname')
user = os.getenv('user')
password = os.getenv('password')
host = os.getenv('host')
port = os.getenv('port')

# Iniciar aplicação
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Conecta ao banco e executa a query
def connect_postgres(query):
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cur = conn.cursor()
    cur.execute(query + f" LIMIT 100 ")
    data = cur.fetchall()
    cur.close()
    conn.close()
    #limita a consulta a 100 requisições
    if len(data) > 100:
        raise ValueError("Excedido o limite de 100 dados simultaneamente")
    return data

# Retorna a resposta da API
def make_response(data=None, message=None):
    success = True if data else False
    response = {
        'success': success,
        'data': data,
        'message': message
    }
    return jsonify(response)

# Um endpoint para retornar o valor de UMA “área colhida” para UM código de município informado e UM ano.
@app.route('/veeries/areacolhida/municipio/<int:municipio_id>/ano/<int:ano>')
def get_area_colhida(municipio_id, ano):
    query = "SELECT municipio_id, ano, valor FROM public.area_colhida WHERE municipio_id = {municipio_id} AND ano = {ano}".format(municipio_id=municipio_id,ano=ano)
    try:
        data = connect_postgres(query)
        # Verificar se o município foi encontrado
        if data:
            # Se os dados forem encontrados
            data_dict = {}
            for idx, row in enumerate(data):
                data_dict[idx] = {
                    'municipio_id': row[0],
                    'ano': row[1],
                    'valor': row[2]
                }
            return make_response(data_dict, 'Requisicao bem sucedida')
        else:
            # Se os dados não forem encontrados
            return make_response(None, 'Dados não encontrados para os parametros informados')
    except ValueError as e:
        return make_response(None, str(e))
    
#Um endpoint para retornar o(s) valor(es) de produtividade informando UM ou MAIS estados brasileiros simultaneamente em UM ano.
@app.route('/veeries/produtividade/estados/<estados>/ano/<int:ano>')
def get_produtividade(estados, ano):
    estados_lista = tuple(estados.split(','))
    query = "SELECT estado, ano, produtividade FROM public.produtividade WHERE estado in {estados_lista} AND ano = {ano}".format(estados_lista=estados_lista,ano=ano)
    try:
        data = connect_postgres(query)
        # Verificar se os dados foram encontrados
        if data:
            # Se os dados forem encontrados
            data_dict = {}
            for idx, row in enumerate(data):
                data_dict[idx] = {
                    'estado': row[0],
                    'ano': row[1],
                    'produtividade': row[2]
                }
            return make_response(data_dict, 'Requisicao bem sucedida')
        else:
            # Se os dados não forem encontrados
            return make_response(None, 'Dados não encontrados para os parametros informados')
    except ValueError as e:
        return make_response(None, str(e))
    
#Um endpoint para retornar múltiplos valores de “quantidade produzida” informando UM ou MAIS municípios E UM ou MAIS anos
@app.route('/veeries/quantidadeproduzida/municipios/<municipios>/anos/<anos>')
def get_quantidade_produzida(municipios, anos):
    anos_lista = [int(ano) for ano in anos.split(',')] 
    municipios_lista = [int(municipio) for municipio in municipios.split(',')] 
    query = "SELECT municipio, estado, ano, valor FROM public.quantidade_produzida WHERE municipio_id IN ({}) AND ano IN ({})".format(','.join(map(str, municipios_lista)), ','.join(map(str, anos_lista)))
    try:
        data = connect_postgres(query)
        # Verificar se os dados foram encontrados
        if data:
            # Se os dados forem encontrados
            data_dict = {}
            for idx, row in enumerate(data):
                data_dict[idx] = {
                    'municipio': row[0],
                    'estado': row[1],
                    'ano': row[2],
                    'valor': row[3]
                }
            return make_response(data_dict, 'Requisicao bem sucedida')
        else:
            # Se os dados não forem encontrados
            return make_response(None, 'Dados não encontrados para os parametros informados')
    except ValueError as e:
        return make_response(None, str(e))





app.run()
