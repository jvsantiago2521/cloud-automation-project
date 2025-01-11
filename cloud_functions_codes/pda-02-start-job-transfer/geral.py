from google.cloud import storage_transfer  # Importa o cliente de Storage Transfer da Google Cloud
import google.auth  # Importa o módulo de autenticação do Google
from google.cloud import bigquery  # Importa o cliente do BigQuery da Google Cloud
from google.protobuf.json_format import MessageToDict  # Importa função para converter mensagem protobuf para dicionário
import json # Importa o módulo json para manipulação de JSON
import os
from google.cloud import storage

project_id = "tlb-dados-abertos"
bigquery_client = bigquery.Client(project=project_id)
credentials, _ = google.auth.default(quota_project_id=project_id)
transfer_client = storage_transfer.StorageTransferServiceClient(credentials=credentials)

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

def run_job(job_name):
    transfer_client.run_transfer_job({'project_id': project_id, 'job_name': job_name})

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

def operation_details(job_name):
    """
    Função para obter detalhes da última operação de transferência.

    Parâmetros:
    - project_id (str): ID do projeto no Google Cloud.
    - job_name (str): Nome do Job de transferência.
    - transfer_client (StorageTransferServiceClient): Cliente de serviço de transferência de armazenamento.

    Retorna:
    - details (dict): Detalhes da última operação de transferência em formato de dicionário.
    """

    # Define o filtro para listar as operações de transferência
    filter_dict = {
        "project_id": project_id,
        "job_names": [job_name]
    }
    filter_str = json.dumps(filter_dict)  # Converte o filtro para uma string JSON

    # Define a requisição para listar as operações
    list_operations_request = {"filter": filter_str}
    
    try:
        # Lista as operações de transferência usando o filtro especificado
        operations = transfer_client.list_operations(request=list_operations_request)
        last_operation = operations.operations[0]  # Obtém a última operação da lista
        
        # Converte a mensagem protobuf da operação para um dicionário
        details = MessageToDict(last_operation.metadata)
        return details
    except IndexError:
        # Caso nenhuma operação seja encontrada, exibe uma mensagem de erro
        print("Nenhuma operação encontrada para o filtro fornecido.")
    except Exception as e:
        # Caso ocorra qualquer outro erro, exibe a mensagem de erro
        print(f"Erro ao listar operações de transferência: {str(e)}")

def links_with_error(job_name):
    failed_links = []
    # Verifica se há detalhes de erro na operação
    if 'errorBreakdowns' in operation_details(job_name):
        for error_detail in operation_details(job_name)['errorBreakdowns']:
            if 'errorLogEntries' in error_detail:
                for error_log_entry in error_detail['errorLogEntries']:
                    failed_url = error_log_entry.get('url')
                    failed_links.append(failed_url)
            else:
                print("Nenhuma amostra de erro encontrada.")
    else:
        print("Nenhum detalhe de erro disponível na última operação.")

    return failed_links

def add_link_pending_table(project_name, links):
    for link in links:
        #Define a query para obter a data e o estado da execução
        query = f"""
            INSERT INTO tlb-dados-abertos.COMANDO.links_pendentes (projeto, link)
            VALUES ('{project_name}', '{link}')
            EXCEPT DISTINCT
            SELECT
            *
            FROM
            `tlb-dados-abertos.COMANDO.links_pendentes`;
        """
        #Executa a query
        query_job = bigquery_client.query(query)

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

def delete_files_in_folder(bucket_name, folder_name):

    # Cria o cliente do Cloud Storage
    storage_client = storage.Client()

    # Obtém o bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Lista todos os blobs (arquivos) no bucket dentro da pasta especificada
    blobs = bucket.list_blobs(prefix=folder_name)

    # Apaga todos os blobs encontrados
    for blob in blobs:
        print(f'Deletando {blob.name}')
        blob.delete()

    print(f'Todos os arquivos em {folder_name} foram deletados.')