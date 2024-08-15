import csv
import pycurl
from io import BytesIO
import pandas as pd
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timedelta
import time
import requests

#Contagem de Tempo
start_time = time.time()

#ARQUIVOS

grupos = r'C:\Files\DADOS LEITURAS TEMPO REAL\BASES\Grupos_Mes_UNLE.csv'
grupos_hoje = r'C:\Files\DADOS LEITURAS TEMPO REAL\BASES\GruposTempoRealUNLE.csv'
grupos_dia = r'C:\Tools\pentaho\Repositorio\LeiturasTempoReal\HISTORICO CONSORCIO\GRUPOS\UNLE\DIARIO\GRUPOS_UNLE.csv'
arq_exec = r'C:\Files\DADOS LEITURAS TEMPO REAL\UNLE\UNLE_TESTE\LeituraUNLE.csv'
output_csv_path = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\LeiturasTempoRealUNLE.csv"


# Data Atual
now = datetime.now()

# Se o dia for maior que 23, acrescentar 1 mês
if now.day > 23:
    now = now.replace(month = now.month % 12 + 1)

# URL API
url = "URL DA API"

# HTTP headers
headers = [
    "Content-Type: application/soap+xml; charset=utf-8",
]

# Request body
body = f"""
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <PegaGrupos xmlns="http://ENDERECO/">
      <mes>{now.month}</mes>
      <ano>{now.year}</ano>
      <chave>CHAVE</chave>
    </PegaGrupos>
  </soap12:Body>
</soap12:Envelope>
"""

# Proxy settings
proxy = "DADOS DO PROXY"

# Proxy authentication
proxy_auth = "SENHA DO PROXY"

# buffer para a resposta
buffer = BytesIO()

# Iniciar o CURL
c = pycurl.Curl()

# atribuir opções do CURL
c.setopt(c.URL, url)
c.setopt(c.HTTPHEADER, headers)
c.setopt(c.POSTFIELDS, body)
c.setopt(c.PROXY, proxy)
c.setopt(c.PROXYUSERPWD, proxy_auth)
c.setopt(c.WRITEDATA, buffer)

# POST request
c.perform()

# receber a resposta
response = buffer.getvalue()

# Parse XML response
root = ET.fromstring(response)


# Inicialize uma lista vazia para armazenar as linhas
rows = []

# Encontre o elemento PegaGruposResult
for result in root.iter('{http://ENDERECO/}PegaGruposResult'):
    # Carregue os dados JSON
    data = json.loads(result.text)

    # Processe cada linha de dados
    for row in data:
        # Converta as strings de data e hora em objetos datetime
        try:
            # Tente analisar a data e hora com frações de segundo
            importacao_datetime = datetime.strptime(row["DataImportacao"], "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            # Se isso falhar, tente analisar a data e hora sem frações de segundo
            importacao_datetime = datetime.strptime(row["DataImportacao"], "%Y-%m-%dT%H:%M:%S")

        leitura_datetime = datetime.strptime(row["DataPrevistaLeitura"], "%Y-%m-%dT%H:%M:%S")

        # Formate os objetos datetime como datas
        importacao_date = importacao_datetime.strftime("%Y-%m-%d")
        leitura_date = leitura_datetime.strftime("%Y-%m-%d")

        # Adicione a linha à lista
        rows.append([row["Mes"], row["Ano"], row["Grupo"], row["Gerencia"], row["Localidade"], row["NomeLocalidade"], row["QtdeCadastros"], importacao_date, leitura_date, row["QtdExecutados"]])

# Converta a lista de linhas em um DataFrame
df = pd.DataFrame(rows, columns=["Mes", "Ano", "Grupo", "Gerencia", "Localidade", "NomeLocalidade", "QtdeCadastros", "DataImportacao", "DataPrevistaLeitura", "QtdExecutados"])

df.to_csv(grupos, sep= ";", index=False)
print("Dados do grupo gerado.")
#print(f"Os dados foram gravados com sucesso em {grupos}")



# Carregar o arquivo csv
#df = pd.read_csv(grupos, delimiter=";")

# Filtrar os dados da tabela "DataPrevistaLeitura" com a data de hoje
hoje = datetime.today().strftime('%Y-%m-%d')
df_filtrado = df[df['DataPrevistaLeitura'] == hoje]

# Armazenar os valores distintos da coluna "Grupo"
grupos_distintos = df_filtrado['Grupo'].unique()

# Subtrair cada um dos valores distintos por -1
grupos_subtracao = grupos_distintos - 1

# Criar uma lista com os valores distintos e os valores com -1
lista_grupos = list(set(list(grupos_distintos) + list(grupos_subtracao)))

# Filtrar o dataframe do output.csv com os valores encontrados na lista
df_final = df[df['Grupo'].isin(lista_grupos)].copy()

# Obter a data de hoje
hoje = datetime.today().date()

# Converter a coluna 'DataHoraServico' para datetime
df_final.loc[:, 'DataPrevistaLeitura'] = pd.to_datetime(df_final['DataPrevistaLeitura'])

# Criar uma nova coluna "Dia" baseada na condição
df_final.loc[:, 'Dia'] = df_final['DataPrevistaLeitura'].apply(lambda x: 'HOJE' if x.date() == hoje else 'ONTEM')

df_final.loc[:, 'DataPrevistaLeitura'] = pd.to_datetime(df_final['DataPrevistaLeitura']).dt.date


# Salvar em um novo arquivo .csv delimitado por ";"
df_final.to_csv(grupos_hoje, sep=';', index=False)
print(f"Arquivo {grupos_hoje} criado.")

df_final_hoje = df_final[df_final['Dia'] == 'HOJE']

df_final_hoje = df_final_hoje[['Mes', 'Ano', 'Grupo', 'Gerencia', 'Localidade']]

df_final_hoje.to_csv(grupos_dia, sep=";", index= False)
print(f"Arquivo {grupos_dia} criado.")











#EXECUÇÃO

# Configurações de proxy
proxy = "DADOS PROXY"
proxy_auth = "SENHA PROXY"

# URL da API
url = "URL DA API"

# Função para criar o corpo da solicitação SOAP
def create_soap_body(mes, ano, gerencia, grupo, localidade):
    soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Body>
        <PegaExecucao xmlns="http://ENDERECO/">
          <mes>{mes}</mes>
          <ano>{ano}</ano>
          <gerencia>{gerencia}</gerencia>
          <grupo>{grupo}</grupo>
          <localidade>{localidade}</localidade>
          <chave>CHAVE</chave>
        </PegaExecucao>
      </soap12:Body>
    </soap12:Envelope>"""
    return soap_body

# Função para enviar a solicitação SOAP usando pycurl
def send_soap_request(body):
    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.PROXY, proxy)
    c.setopt(c.PROXYUSERPWD, proxy_auth)
    c.setopt(c.HTTPHEADER, ["Content-Type: application/soap+xml; charset=utf-8"])
    c.setopt(c.POST, 1)
    c.setopt(c.POSTFIELDS, body)
    c.setopt(c.WRITEDATA, buffer)
    c.perform()
    c.close()
    return buffer.getvalue().decode('utf-8')

# Função para extrair e analisar a resposta JSON do envelope SOAP
def parse_soap_response(response):
    # Parse o XML
    root = ET.fromstring(response)
    
    # Encontre o elemento PegaExecucaoResult
    pega_execucao_result = root.find(".//{http://ENDERECO/}PegaExecucaoResult")
    
    if pega_execucao_result is not None:
        # Converta o texto JSON para um objeto Python
        result_json = json.loads(pega_execucao_result.text)
        return result_json
    else:
        return None

# DataFrame para armazenar todas as respostas
all_data = pd.DataFrame()

# Ler o arquivo CSV delimitado por ponto e vírgula e iterar sobre cada linha
with open(grupos_hoje, "r") as file:
    csv_reader = csv.DictReader(file, delimiter=';')
    for row in csv_reader:
        # Criar o corpo da solicitação SOAP
        soap_body = create_soap_body(row['Mes'], row['Ano'], row['Gerencia'], row['Grupo'], row['Localidade'])
        
        # Enviar a solicitação SOAP e obter a resposta
        response = send_soap_request(soap_body)
        
        # Analisar a resposta
        data = parse_soap_response(response)
        
        if data:
            # Converter os dados JSON para um DataFrame
            df = pd.DataFrame(data)
            
            # Criar um DataFrame para os dados do CSV
            csv_df = pd.DataFrame([row])
            
            # Repetir os dados do CSV para cada linha da resposta da API
            csv_df = pd.concat([csv_df] * len(df), ignore_index=True)
            
            # Concatenar os DataFrames: primeiro os dados do CSV, depois os dados da resposta
            combined_df = pd.concat([csv_df.reset_index(drop=True), df.reset_index(drop=True)], axis=1)
            
            # Adicionar os dados combinados ao DataFrame principal
            all_data = pd.concat([all_data, combined_df], ignore_index=True)

# Reordenar as colunas conforme a estrutura desejada
all_data['UNIDADE'] = 'UNLE'

integer_columns = ['MatriculaFuncionario', 'Ocorrencia1Anterior1', 'Ocorrencia2Anterior1', 'LeituraMinima', 'LeituraMaxima', 'LeituraReal', 'Ocorrencia01', 'Ocorrencia02', 'VolumeMedido']

# Converte as colunas para inteiros
for col in integer_columns:
    all_data[col] = all_data[col].astype('Int64')

column_order = [
    'CargaId', 'MatriculaFuncionario', 'NomeFuncionario', 'Gerencia', 'Grupo', 'Localidade', 'NomeLocalidade',
    'Setor', 'Rota', 'Face', 'Sequencia', 'Coletor', 'MatriculaClienteImovel', 'NumeroMedidorAbnt',
    'Latitude', 'Longitude', 'LeituraRealAnterior', 'Ocorrencia1Anterior1', 'Ocorrencia2Anterior1',
    'LeituraMinima', 'LeituraMaxima', 'LeituraReal', 'ForaFaixa', 'Ocorrencia01', 'Ocorrencia02',
    'VolumeMedido', 'QtdImpressoes', 'ProcessamentoColetor', 'Categorias', 'Economias',
    'DataHoraServico', 'DataHoraRetornoCopasa', 'UNIDADE', 'Dia'
]

# Verificar se todas as colunas desejadas estão presentes no DataFrame
missing_columns = [col for col in column_order if col not in all_data.columns]
if missing_columns:
    # Adicionar colunas faltantes com valores NaN
    for col in missing_columns:
        all_data[col] = pd.NA

# Reordenar as colunas do DataFrame final
all_data = all_data[column_order]

# Gerar o arquivo CSV final delimitado por ";"
all_data.to_csv(output_csv_path, sep=';', index=False)







#TEMPO REAL

arq = r"C:\Files\DADOS LEITURAS TEMPO REAL\BASES\LeiturasTempoRealUNLE.csv"

df = pd.read_csv(arq, delimiter=";", low_memory=False)

#PROGRAMADAS
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE')]
programadas_total = len(df_filtrado)

#REALIZADAS
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & (df['VolumeMedido'].notna())]
realizadas_total = df_filtrado['MatriculaClienteImovel'].nunique()

#PERCENTUAL TOTAL DE REALIZADAS
perc_leituras_total = round((realizadas_total / programadas_total ) * 100,ndigits=None)
perc_leituras_total_str = str(round((realizadas_total / programadas_total ) * 100,ndigits=None)) + '%'

# Lista de gerências
gerencias = ['GRCA', 'GRDT', 'GRTO', 'GRIP', 'GRAL']

# Inicializando dicionários para armazenar os resultados
programadas = {}
realizadas = {}
perc_leituras = {}

for gerencia in gerencias:
    # Filtrando o DataFrame para cada gerência
    df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & (df['Gerencia'] == gerencia)]
    programadas[gerencia] = len(df_filtrado)
    
    df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & (df['VolumeMedido'].notna()) & (df['Gerencia'] == gerencia)]
    realizadas[gerencia] = df_filtrado['MatriculaClienteImovel'].nunique()
    
    perc_leituras[gerencia] = round((realizadas[gerencia] / programadas[gerencia] ) * 100,ndigits=None)

valor_grca = perc_leituras['GRCA']
grca = 'GRCA'
valor_grdt = perc_leituras['GRDT']
grdt = 'GRDT'
valor_grto = perc_leituras['GRTO']
grto = 'GRTO'
valor_grip = perc_leituras['GRIP']
grip = 'GRIP'
valor_gral = perc_leituras['GRAL']
gral = 'GRAL'




#QTD OCORRENCIA 05
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & ((df['Ocorrencia01'] == 5) | (df['Ocorrencia02'] == 5))]
ocorr_05 = len(df_filtrado)

#QTD IMPEDIMENTOS
valores = [90, 45, 43, 22, 17, 15, 11, 8, 5, 4, 3, 2, 1]
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & ((df['Ocorrencia01'].isin(valores)) | (df['Ocorrencia02'].isin(valores)))]
impedimentos = len(df_filtrado)

#PERCENTUAL DE IMPEDIMENTOS
perc_impedimentos = round((impedimentos / realizadas_total ) * 100,ndigits=1)
perc_impedimentos_str = str(round((impedimentos / realizadas_total ) * 100,ndigits=1)) + '%'

#MÉDIA DE LEITURAS PROGRAMADAS POR LEITURISTA
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE')]
qtd_leituristas = df_filtrado['NomeFuncionario'].nunique(dropna=True)
df_filtrado_funcionario = df_filtrado[df_filtrado['NomeFuncionario'].notna()]
qtd_leituras = len(df_filtrado_funcionario['MatriculaClienteImovel'])
avg_leituras = round(qtd_leituras / qtd_leituristas, ndigits=None)

#MÉDIA DE LEITURAS REALIZADAS POR LEITURISTA
avg_leituristas_realizadas = round(realizadas_total / qtd_leituristas, ndigits=None)



#LEITURISTAS COM MAIS OCORRÊNCIAS
df_filtrado = df[(df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE')]

df_filtered = df_filtrado[(df_filtrado['NomeFuncionario'].notna()) & (df_filtrado['Ocorrencia01'].notna()) & (df_filtrado['Ocorrencia01'] != 0) | (df_filtrado['Ocorrencia01'].notna()) & (df_filtrado['Ocorrencia01'] == 0) & (df_filtrado['Ocorrencia02'] != 0)]

grouped = df_filtered.groupby('NomeFuncionario')['Ocorrencia01'].count()

top_5 = grouped.sort_values(ascending=False).head(5)

nome01, Total_nome01 = top_5.index[0], top_5.iloc[0]
nome02, Total_nome02 = top_5.index[1], top_5.iloc[1]
nome03, Total_nome03 = top_5.index[2], top_5.iloc[2]
nome04, Total_nome04 = top_5.index[3], top_5.iloc[3]
nome05, Total_nome05 = top_5.index[4], top_5.iloc[4]



#OCORRENCIAS
def ocorrencias(df, ocorrencia):
    condicao = (df['MatriculaClienteImovel'].notna()) & (df['Dia'] == 'HOJE') & ((df['Ocorrencia01'] == ocorrencia) | (df['Ocorrencia02'] == ocorrencia))
    return len(df[condicao])
OC1 = ocorrencias(df,1)
OC2 = ocorrencias(df,2)
OC3 = ocorrencias(df,3)
OC4 = ocorrencias(df,4)
OC5 = ocorrencias(df,5)
OC8 = ocorrencias(df,8)
OC11 = ocorrencias(df,11)
OC15 = ocorrencias(df,15)
OC17 = ocorrencias(df,17)
OC22 = ocorrencias(df,22)
OC43 = ocorrencias(df,43)
OC45 = ocorrencias(df,45)
OC90 = ocorrencias(df,90)


#HORA ATUAL
now = datetime.now()
now_minus_3 = now + timedelta(hours=3)
data_hora_atual = now_minus_3.strftime("%Y-%m-%dT%H:%M:%S.000Z")


data = [
    {
        "programadas_total": programadas_total,
        "realizadas_total": realizadas_total,
        "perc_leituras_total": perc_leituras_total,
        "valor_grca": valor_grca,
        "grca": grca,
        "valor_grdt": valor_grdt,
        "grdt": grdt,
        "valor_grto": valor_grto,
        "grto": grto,
        "valor_grip": valor_grip,
        "grip": grip,
        "valor_gral": valor_gral,
        "gral": gral,
        "ocorr_05": ocorr_05,
        "impedimentos": impedimentos,
        "perc_impedimentos": perc_impedimentos,
        "avg_leituras": avg_leituras,
        "avg_leituristas_realizadas": avg_leituristas_realizadas,
        "min_indicador": 0,
        "max_indicador": programadas_total,
        "datetime_now": data_hora_atual,
        "max_perc": 100,
        "OC1": OC1,
        "OC2": OC2,
        "OC3": OC3,
        "OC4": OC4,
        "OC5": OC5,
        "OC8": OC8,
        "OC11": OC11,
        "OC15": OC15,
        "OC17": OC17,
        "OC22": OC22,
        "OC43": OC43,
        "OC45": OC45,
        "OC90": OC90
    }
]

data1 = [
    {
       "nome01": nome01,
       "Total_nome01": int(Total_nome01) 
    }
]

data2 = [
    {
       "nome01": nome02,
       "Total_nome01": int(Total_nome02) 
    }
]

data3 = [
    {
       "nome01": nome03,
       "Total_nome01": int(Total_nome03) 
    }
]

data4 = [
    {
       "nome01": nome04,
       "Total_nome01": int(Total_nome04) 
    }
]


data5 = [
    {
       "nome01": nome05,
       "Total_nome01": int(Total_nome05) 
    }
]


url = "URL DO CONJUNTO DE DADOS"
url2 = "URL DO CONJUNTO DE DADOS"

headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, data=json.dumps(data), headers=headers)
time.sleep(0.2)
response = requests.post(url2, data=json.dumps(data1), headers=headers)
time.sleep(0.2)
response = requests.post(url2, data=json.dumps(data2), headers=headers)
time.sleep(0.2)
response = requests.post(url2, data=json.dumps(data3), headers=headers)
time.sleep(0.2)
response = requests.post(url2, data=json.dumps(data4), headers=headers)
time.sleep(0.2)
response = requests.post(url2, data=json.dumps(data5), headers=headers)

if response.status_code == 200:
    print("Dados enviados com sucesso!")
else:
    print(f"Erro ao enviar dados: {response.content}")




# Tempo de término
end_time = time.time()

# Tempo de execução
execution_time = end_time - start_time

print(f"Arquivo CSV gerado com sucesso: {output_csv_path}")

print(f"O tempo de execução do código é {execution_time} segundos.")
