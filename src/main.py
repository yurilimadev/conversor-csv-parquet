#Bibliotecas Instaladas

import json
import io
import os
import mysql.connector
import pyarrow.parquet as pq
import pandas as pd

#função de conversão

def csv_parquet_powerbi(caminho):
  df_teste = pd.read_csv(caminho)
  buffer = io.BytesIO()
  df_teste.to_parquet(buffer, engine='pyarrow', compression='snappy')
  return caminho, buffer.getvalue(), os.path.getsize(caminho) / (1024 * 1024), len(buffer.getvalue()) / (1024 * 1024),  json.dumps({"colunas": df_teste.columns.tolist()})

#carregando arquivo de teste

csv_comprimido = csv_parquet_powerbi('data/california_housing_test.csv')

#checagem se parquet foi criado corretamente

try:
  pq.read_table(io.BytesIO(csv_comprimido[1]))
  print('Parquet Valido')
except Exception as e:
  print(f'Parquet Invalido:{e}')

#conecta com o banco de dados

config = mysql.connector.connect(
    host=os.getenv('HOST'),
    user=os.getenv('USER'),
    password=os.getenv('SENHA'),
    database=os.getenv('DB')
)
cursor = config.cursor()

#faz a inserção

query = """
INSERT INTO arquivos_csv
(nome_arquivo, dados, tamanho_original_mb, tamanho_blob_mb, colunas_json)
VALUES (%s, %s, %s, %s, %s)
"""

cursor.execute(query, (
    csv_comprimido[0].split('/')[-1],  # Nome do arquivo
    csv_comprimido[1],
    csv_comprimido[2],
    csv_comprimido[3],
    csv_comprimido[4])  # Metadados para Power BI
)

config.commit()
config.close()

#saída final do resultado da conversão

print(f"Arquivo {csv_comprimido[0]} inserido como BLOB. Economia: {csv_comprimido[2] - csv_comprimido[3]:.2f} MB")
