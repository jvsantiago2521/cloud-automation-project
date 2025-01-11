from google.cloud import bigquery  # Importa o cliente do BigQuery da Google Cloud
import json  # Importa o módulo json para manipulação de JSON
import time
from google.cloud import storage  # Importa o cliente do Google Cloud Storage
import os

project_id = "tlb-dados-abertos"
bigquery_client = bigquery.Client(project=project_id)

#Função para obter o conteúdo JSON de uma coluna no BigQuery
def content_json(column, project_name):
    """
    Parâmetros:
    column (str): O nome da coluna a ser consultada (Cada coluna é um tipo de json).
    project_name (str): O nome do projeto para filtrar na consulta.
    """

    #Define a query para obter o conteúdo JSON da coluna especificada
    query = f"""
        SELECT {column} FROM tlb-dados-abertos.COMANDO.json
        WHERE Projeto = '{project_name}';
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)
    #Obtém o resultado da query
    result = next(query_job.result(), None)
    #Obtém o conteúdo JSON da coluna especificada
    content_json = getattr(result, column)
    #Retorna o conteúdo JSON
    return content_json

#Função para obter a data e o estado da execução do BigQuery
def query_execution_manager(project_name):
    """
    Parâmetros:
    project_name (str): O nome do projeto para filtrar na consulta.
    """
 
    #Define a query para obter a data e o estado da execução
    query = f"""
        SELECT Data, Estado FROM tlb-dados-abertos.COMANDO.gestor_execucoes
        WHERE Projeto = '{project_name}'
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)
    #Obtém o resultado da query
    result = next(query_job.result(), None)
    #Retorna o resultado da query
    return result

#Função para atualizar a data e o estado da execução no BigQuery
def update_date_state(next_state, current_date, project_name):
    """
    Parâmetros:
    next_state (int): O número correspondente ao próximo estado.
    current_date (datetime.date): A data atual.
    project_name (str): O nome do projeto para filtrar na consulta.
    """

    #Define a query para atualizar a data e o estado da execução
    query = f"""
        UPDATE tlb-dados-abertos.COMANDO.gestor_execucoes
        SET Estado = {next_state}, Data = '{current_date}'
        WHERE Projeto = '{project_name}'
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)

def organizer(bucket_name, corrected_path, subfolder_path, project_name):

    # Cria um cliente do Google Cloud Storage
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = storage_client.list_blobs(bucket_name, prefix=subfolder_path)

    # Print para verificar os blobs listados
    print(f'Blobs encontrados no prefixo {subfolder_path}:')
    for blob in blobs:
        print(blob.name)

    # Lista todos os blobs no caminho das subpastas novamente
    blobs = storage_client.list_blobs(bucket_name, prefix=subfolder_path)

    for blob in blobs:
        # Obtém o caminho completo do blob
        blob_path = blob.name
        print(f'Processing blob: {blob_path}')
        
        # Extrai o nome da subpasta e do arquivo
        subfolder_name = os.path.basename(os.path.dirname(blob_path))
        file_name = os.path.basename(blob_path)
        
        # Renomeia o arquivo com o nome da subpasta
        if project_name == "DADOS ABERTOS":
            new_file_name = f'{subfolder_name}'
            new_blob_name = os.path.join(corrected_path, new_file_name)
        else:
            new_file_name = f'{file_name}'
            new_blob_name = os.path.join(corrected_path, new_file_name)

        # Move o arquivo para a pasta principal
        bucket.rename_blob(blob, new_blob_name)
        print(f'Moved {blob.name} to {new_blob_name}')
        
        # Espera para evitar atingir o limite de taxa
        time.sleep(1.0)

# Define uma função para listar arquivos em uma pasta específica de um bucket do Google Cloud Storage
def list_files(bucket_name, folder):
    # Lista para armazenar os nomes dos arquivos
    file_list = []

    # Obtém o bucket
    bucket = storage.Client().get_bucket(bucket_name)

    # Lista os blobs dentro da pasta específica
    blobs = bucket.list_blobs(prefix=folder)

    # Itera sobre os blobs e adiciona os nomes dos arquivos à lista
    for blob in blobs:
        # Adiciona o nome do arquivo somente se não for uma pasta (evita itens vazios)
        if not blob.name.endswith('/'):
            file_list.append(os.path.basename(blob.name))
       
        
    return file_list

def create_files_states(lista_arquivos, project_name):

    query1 = f"""
        DELETE FROM `tlb-dados-abertos.COMANDO.gestor_arquivos`
        WHERE projeto = '{project_name}';
    """

    query_job = bigquery_client.query(query1)
    query_job.result()

    values = ', '.join([f"('{project_name}', '{arquivo}', 0)" for arquivo in lista_arquivos])
    query2 = f"""
        INSERT INTO `tlb-dados-abertos.COMANDO.gestor_arquivos` (projeto, arquivo, estado)
        VALUES {values};
    """

    query_job = bigquery_client.query(query2)
    query_job.result()
    print("Tabela alterada!")
