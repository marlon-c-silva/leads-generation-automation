# Leads Generation Automation

Pipeline de prospecção B2B para localizar empresas, identificar decisores e enriquecer dados com integração a Gemini, minhareceita.org, Apify, Serper e Kipflow.

## Visão geral

Este projeto orquestra uma DAG do Apache Airflow responsável por:

- carregar parâmetros de busca via variáveis do Airflow;
- buscar contexto web com suporte a Google Search nativo do Gemini ou fallback com Serper;
- extrair empresas e decisores em formato estruturado;
- complementar dados cadastrais via minhareceita.org;
- raspar perfis do LinkedIn com Apify;
- enriquecer emails com Kipflow;
- persistir o resultado final em PostgreSQL;
- registrar a execução em uma tabela de histórico da pipeline.

## Estrutura do projeto

```text
.
├── dag_prospecao_b2b.py          # DAG principal do Airflow
├── airflow_variables.example.json # exemplo de variáveis do Airflow
├── requirements.txt              # dependências do projeto
├── .gitignore                   # arquivos locais e segredos
├── utils/
│   └── api_clients.py           # integrações e parsing estruturado
└── venv/                        # ambiente virtual local (ignorado no git)
```

## Requisitos

- Python 3.11+
- Apache Airflow 2.10.x
- PostgreSQL
- Tokens/keys de:
  - Gemini
  - Serper (opcional, para fallback de busca web)
  - Apify
  - Kipflow

## Instalação local

1. Crie um ambiente virtual:

```bash
python -m venv venv
```

2. Ative o ambiente:

```bash
# Linux/macOS
source venv/bin/activate

# Windows PowerShell
.\venv\Scripts\Activate.ps1
```

3. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Configuração de variáveis

Use o arquivo [airflow_variables.example.json](airflow_variables.example.json) como base para criar as variáveis no Airflow.

### Exemplo de importação via CLI

```bash
airflow variables import airflow_variables.example.json
```

### Variáveis principais

- `PROSPECCAO_PROMPT_TEMPLATE`
- `PROSPECCAO_SEGMENTO_EMPRESA`
- `PROSPECCAO_TAMANHO_EMPRESA`
- `PROSPECCAO_LOCALIZACAO_EMPRESA`
- `PROSPECCAO_QTD_EMPRESAS`
- `PROSPECCAO_QTD_DECISORES`
- `PROSPECCAO_CARGOS_DECISORES`
- `GEMINI_API_KEY`
- `GEMINI_MODEL` (ex.: `gemini-3.5-flash-lite`)
- `USE_GOOGLE_SEARCH_TOOL`
- `WEB_SEARCH_PROVIDER`
- `SERPER_API_KEY`
- `SERPER_NUM_RESULTS`
- `APIFY_API_TOKEN`
- `APIFY_LINKEDIN_ACTOR_ID`
- `KIPFLOW_API_KEY`
- `KIPFLOW_BASE_URL`
- `CNPJ_BASE_URL`
- `POSTGRES_CONN_ID`
- `POSTGRES_SCHEMA`
- `POSTGRES_TABLE`

> O arquivo de exemplo já contém os nomes esperados pela DAG e valores iniciais para facilitar a configuração.

## Configuração da conexão PostgreSQL

No Airflow, configure uma conexão chamada `postgres_default` ou ajuste o valor de `POSTGRES_CONN_ID` para a sua conexão real.

### Via interface do Airflow

1. Acesse a UI do Airflow.
2. Vá em `Admin > Connections`.
3. Crie a conexão `postgres_default`.
4. Defina:
   - Connection ID: `postgres_default`
   - Connection Type: `Postgres`
   - Host
   - Schema
   - Login
   - Password
   - Port

## Implantação da DAG no Airflow

1. Copie o arquivo `dag_prospecao_b2b.py` para a pasta de DAGs do Airflow.
2. Garanta que a pasta `utils` também esteja acessível no ambiente do Airflow, ou que o módulo seja importado corretamente no `PYTHONPATH`.
3. Reinicie ou recarregue o Airflow.
4. Ative a DAG na interface.

Exemplo de pasta no Ubuntu:

```bash
/home/airflow/airflow/dags/
```

Se o Airflow estiver em um ambiente virtual, a estrutura pode ser similar a:

```bash
/home/airflow/airflow/dags/
/home/airflow/airflow/dags/utils/
```

## Execução da DAG

### Via interface do Airflow

- Acesse a janela da DAG;
- clique em `Trigger DAG`;
- acompanhe os logs das tasks;
- valide o resultado no PostgreSQL e nos logs finais.

### Via CLI

```bash
airflow dags trigger prospeccao_b2b
```

## Como funciona a lógica de busca

A DAG usa uma estratégia híbrida:

- se a modelagem configurada for Gemini e suportar Google Search, usa o mecanismo nativo;
- se a modelagem for Gemma ou a opção estiver desabilitada, usa busca externa via Serper;
- o contexto web é incorporado ao prompt para manter a qualidade da extração.

## Saída esperada

O resultado final é persistido em JSONB em uma tabela PostgreSQL. O payload inclui:

- total de empresas;
- total de contatos enriquecidos;
- lista de empresas com decisores, dados cadastrais, perfis e contatos.

## Observações importantes

- A DAG depende de chaves reais e de acesso válido aos serviços externos.
- O modelo Gemini e a busca Google/Serper devem ser escolhidos de acordo com o uso real do projeto.
- Em ambientes de produção, prefira armazenar segredos em variáveis do Airflow ou um gerenciador de segredos seguro.

## Troubleshooting

### DAG não aparece no Airflow

- confirme se o arquivo foi enviado para a pasta correta de DAGs;
- verifique se não há erro de importação do módulo `utils`;
- revise os logs de inicialização do Airflow.

### Erro de variável ausente

- importe novamente o arquivo JSON de exemplo;
- valide o nome exato das variáveis;
- confirme que as chaves foram preenchidas.

### Busca web sem retorno

- verifique a chave do Serper;
- confirme `WEB_SEARCH_PROVIDER` e `USE_GOOGLE_SEARCH_TOOL`;
- valide se o modelo Gemini escolhido suporta o tipo de busca configurado.

## Licença

Este projeto é mantido para uso interno/operacional e pode ser adaptado conforme a necessidade da organização.
