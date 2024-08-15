# leituras_tempo_real_API
 OBTENÇÃO DE DADOS DE LEITURAS VIA API

### Importações e Definições Iniciais
O código começa com as importações necessárias de bibliotecas para manipulação de arquivos, comunicação com APIs, e análise de dados:

- `csv`, `pycurl`, `requests`: para manipulação de arquivos CSV, envio de solicitações SOAP via cURL, e requisições HTTP.
- `pandas`: para análise e manipulação de dados.
- `xml.etree.ElementTree`: para análise de respostas XML.
- `json`: para trabalhar com dados JSON.
- `datetime`, `timedelta`, `time`: para manipulação de datas e tempos.

### Carregamento e Manipulação de Arquivos
Define-se vários caminhos para arquivos que serão usados e gerados pelo código:

- **Grupos e leituras**:
  - `grupos`: Arquivo que conterá os dados dos grupos.
  - `grupos_hoje`: Dados filtrados dos grupos para o dia atual.
  - `grupos_dia`: Grupos do dia atual.
  - `arq_exec`: Arquivo para salvar leituras realizadas.
  - `output_csv_path`: Arquivo CSV final contendo todos os dados.

### Data Atual e Ajuste de Mês
O código verifica a data atual e, se o dia for maior que 23, ajusta o mês para o próximo.

### Configuração de Requisição SOAP
Criação de uma requisição SOAP para consultar grupos em uma API específica, usando pycurl para envio da requisição.

### Processamento da Resposta
Após receber a resposta da API em formato XML, o código:
- Analisa o XML.
- Extrai os dados necessários.
- Converte as strings de data em objetos datetime e formata como datas.
- Cria um DataFrame com os dados processados e salva em `grupos`.

### Filtragem dos Grupos
O código carrega o arquivo de grupos e filtra as leituras previstas para o dia atual. Gera uma lista de grupos únicos e aplica algumas transformações, como subtração de 1 de cada grupo.

### Execução das Leituras
Depois de filtrar os grupos, o código:
- Envia uma nova solicitação SOAP para obter as leituras.
- Processa a resposta e concatena os dados com as informações de grupos.
- Salva o DataFrame final em `output_csv_path`.

### Estatísticas em Tempo Real
O código então calcula várias estatísticas, como:
- Total de leituras programadas e realizadas.
- Percentual de leituras realizadas.
- Percentual de impedimentos (leituras que não puderam ser realizadas devido a alguma ocorrência).
- Média de leituras programadas e realizadas por leiturista.
- Leituras com mais ocorrências.

### Geração do Arquivo Final
O código finaliza reorganizando as colunas no DataFrame final e salvando os dados em um arquivo CSV, utilizando as estatísticas calculadas para gerar os relatórios de leituras realizadas em tempo real.

---

Este código automatiza o processo de consulta de uma API SOAP para obter dados de leituras, processá-los, e gerar relatórios detalhados sobre as leituras realizadas, incluindo várias estatísticas e informações em tempo real.