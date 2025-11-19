from google.cloud import bigquery
from datetime import datetime, date
import sys
import ast
import pandas as pd
import pytz
from databricks_cli.sdk import ApiClient, SecretService
import os
import warnings
warnings.filterwarnings("ignore")
import json
import requests

import base64


# ========================
# CONFIGURAÇÕES
# ========================

PROJECT = 'telefonica-digitalsales'
DATASET = 'coe_testes'
TABLE = 'b2c_shinobi_fixa_vivototal_vendas' 




def get_databricks_secret(scope, key):
    """Recupera secret do Databricks e retorna como dict"""
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
    
    url = f"{DATABRICKS_HOST}/api/2.0/secrets/get"
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    params = {"scope": scope, "key": key}
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    secret_value = response.json()["value"]
    decoded_secret = base64.b64decode(secret_value).decode('utf-8')
    
    return json.loads(decoded_secret)

# Usar em qualquer lugar do código
credentials = get_databricks_secret(scope="ecp", key="gcp-sa-coe-databricks-key")
BQ_CLIENT = bigquery.Client.from_service_account_info(credentials)

print("✓ Cliente BigQuery configurado!")


#BQ_CLIENT = bigquery.Client()
#BQ_CLIENT = bigquery.Client.from_service_account_info(ast.literal_eval(dbutils.secrets.get(scope="ecp", key="gcp-sa-coe-databricks-key")))


DATE = datetime.today().strftime('%Y-%m-%d')

# ========================
# QUERY
# ========================
QUERY = f"""

    WITH table as (
    SELECT 
        FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%d-%m-%Y', data_criacao)) AS data_pedido_ajustada_formatada,
        FORMAT_DATE('%Y-%m-%d', SAFE.PARSE_DATE('%d-%m-%Y', data_da_modificacao)) AS data_da_modificacao_formatada,
        TO_HEX(SHA256(CONCAT(codigo_postal, ',s)S.X-p;SdwsA2&lR.dIy|SCg}}bZ1{{&7y^%kpk2u9V+{{mEO%n3HccBYJIKhFujb'))) AS codigo_postal_hash,
        *
    FROM shinobi-vivo.vivo_total.relatorio_vivo_total_vendas
    )
    SELECT * FROM table
    WHERE data_pedido_ajustada_formatada = '{DATE}' 
"""

# ========================
# FUNÇÕES
# ========================
def log(msg):
    print(f"[{datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S')}] {msg}")
    sys.stdout.flush()


def pandas_bq_query_df(query):
  return BQ_CLIENT.query(query).to_dataframe()


def bq_query(query):
  # API request
  query_job = BQ_CLIENT.query(query)
  # Waits for query to finish
  return query_job.result()



def delete(table, date, project, dataset):
  log(f"Deletando registros da tabela {project}.{dataset}.{table}. Período {date}")

  query = f"""
    DELETE FROM `{project}.{dataset}.{table}`
    WHERE data_pedido_ajustada_formatada = '{date}'
  """

  bq_query(query)



def transform(df):
  df.columns.tolist()

  df['data_pedido_ajustada'] = pd.to_datetime(df['data_pedido_ajustada_formatada']).dt.date
  df['codigo_postal'] = df['codigo_postal_hash']
  df['hora_pedido'] = df['hora_criacao']	
  df['data_da_modificacao'] = df['data_da_modificacao_formatada']
  df['created_at'] = datetime.now(pytz.timezone('America/Sao_Paulo')).replace(tzinfo=None)

  df = df.drop(columns=["codigo_postal_hash", "data_criacao", "hora_criacao"])



  return df



## Bigquery for Pandas ##
def ingest_bigquery_for_pd(table, df, project, date, dataset='coe_datalake', schema=[], write_disposition='WRITE_APPEND'):

  delete(table, date, project, dataset)


  log(f"Carregando dados na tabela {project}.{dataset}.{table}. Período {date}")

  dataset_ref = BQ_CLIENT.dataset(dataset)
  dataset_table = dataset_ref.table(table)

  settings_load = bigquery.LoadJobConfig(
    write_disposition=write_disposition,
    schema=schema
  )
  
  # API request
  query_job = BQ_CLIENT.load_table_from_dataframe(df, dataset_table, job_config=settings_load)
  
  # Waits for query to finish
  query_job.result()



# ========================
# MAIN
# ========================
if __name__ == "__main__":
    log("=" * 60)
    log("INICIANDO BQ SHINOBI HORA HORA")
    log(f"{PROJECT}.{DATASET}.{TABLE}")
    log("=" * 60)
    
    try:
        df = pandas_bq_query_df(QUERY)
        df_t = transform(df)
        ingest_bigquery_for_pd(TABLE, df_t, PROJECT, DATE, DATASET)
        
        log("=" * 60)
        log("✓ PROCESSO CONCLUÍDO COM SUCESSO!")
        log("=" * 60)
        
    except Exception as e:
        log(f"✗ ERRO: {str(e)}")
        sys.exit(1)
